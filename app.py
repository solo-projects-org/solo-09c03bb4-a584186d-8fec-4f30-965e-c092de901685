"""Flask front end for the Iceberg Parquet field-ID backfiller.

The page accepts a valid-file manifest in the existing CSV/JSON format, the
Parquet replicas named in that manifest, and the JSON text of the table
property ``schema.name-mapping.default``.  On success the whole batch is
rewritten and streamed back as a ZIP whose entries keep the original
uploaded file names.  Any failure renders the offending file(s) and
column(s) and never offers a download.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from flask import Flask, g, render_template, request, send_file

from checker.engine import BackfillResult, run_backfill
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.namemapping import parse_name_mapping

ROOT = Path(__file__).resolve().parent
SAMPLE_DIR = ROOT / "sample"
SAMPLE_MAPPING_PATH = SAMPLE_DIR / "name-mapping.json"
ZIP_NAME = "field-ids-backfill.zip"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (
    int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024
)


@app.teardown_appcontext
def _cleanup_temp_dirs(_error=None):
    """Remove every temp directory created during this request.

    Registered on ``g`` so that the cleanup runs once the application
    context pops, i.e. after the response (including a streamed file
    download) has been handed to the WSGI server.
    """
    temp_dirs = getattr(g, "_field_id_temp_dirs", None)
    if not temp_dirs:
        return
    for temp_dir in temp_dirs:
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dirs.clear()


def _new_temp_dir() -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="iceberg-field-id-"))
    g.setdefault("_field_id_temp_dirs", []).append(temp_dir)
    return temp_dir


def _sample_mapping_text() -> str:
    return SAMPLE_MAPPING_PATH.read_text(encoding="utf-8")


def _save_uploads(file_storages, temp_dir: Path) -> dict[str, Path]:
    uploaded: dict[str, Path] = {}
    for file_storage in file_storages:
        if file_storage is None or not file_storage.filename:
            continue
        # Browsers occasionally send a client-side path; keep only the name.
        name = os.path.basename(file_storage.filename.replace("\\", "/")).strip()
        if not name:
            raise CheckError("上传的 Parquet 文件名为空。")
        if name in uploaded:
            raise CheckError(
                f"上传文件名重复：{name}。清单中的 uploaded_filename 必须唯一。",
                files=[name],
            )
        destination = temp_dir / f"upload-{len(uploaded) + 1:04d}.parquet"
        try:
            file_storage.save(destination)
        except Exception as exc:
            raise CheckError(
                f"无法保存上传文件 “{name}”：{exc}", files=[name]
            ) from None
        if destination.stat().st_size == 0:
            raise CheckError(f"上传文件 “{name}” 为空。", files=[name])
        uploaded[name] = destination
    return uploaded


def _sample_uploaded_paths(manifest) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    missing: list[str] = []
    for entry in manifest.required_entries:
        candidate = SAMPLE_DIR / entry.uploaded_filename
        if candidate.is_file():
            paths[entry.uploaded_filename] = candidate
        else:
            missing.append(entry.uploaded_filename)
    if missing:
        raise CheckError(
            "内置样例缺少以下 Parquet 副本：" + "、".join(missing),
            files=missing,
        )
    return paths


def _build_zip(output_dir: Path, zip_path: Path, ordered_names: list[str]) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ordered_names:
            child = output_dir / name
            if child.is_file():
                # arcname is the original uploaded file name.
                archive.write(child, arcname=name)


def _render(
    *,
    error: CheckError | None = None,
    mapping_text: str = "",
    source: str = "",
    sample_manifest=None,
    status: int = 200,
):
    sample_mapping = ""
    if source == "sample" and sample_manifest is None:
        sample_mapping = _sample_mapping_text()
    return (
        render_template(
            "index.html",
            error=error,
            mapping_text=mapping_text,
            source=source,
            sample_mapping=sample_mapping,
            sample_manifest=sample_manifest,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
        ),
        status,
    )


@app.get("/")
def index():
    return _render()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/sample")
def sample_page():
    """Prefill the form with the bundled manifest and name mapping."""
    try:
        manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
        mapping_text = _sample_mapping_text()
        # Validate the bundled sample up front so the page can explain it.
        parse_name_mapping(mapping_text)
    except CheckError as exc:
        return _render(error=exc, source="sample", status=500)
    return _render(
        mapping_text=mapping_text,
        source="sample",
        sample_manifest=manifest,
    )


def _run_and_zip(
    *,
    manifest,
    uploaded_paths: dict[str, Path],
    mapping_text: str,
):
    name_mapping = parse_name_mapping(mapping_text)
    temp_root = _new_temp_dir()
    output_dir = temp_root / "out"
    output_dir.mkdir()
    result: BackfillResult = run_backfill(
        manifest, uploaded_paths, name_mapping, output_dir
    )
    zip_path = temp_root / ZIP_NAME
    ordered_names = [
        file_result.entry.uploaded_filename
        for file_result in result.file_results
    ]
    _build_zip(output_dir, zip_path, ordered_names)

    response = send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=ZIP_NAME,
    )
    # Summary headers let tests/clients verify the batch without unzipping.
    response.headers["X-Files-Rewritten"] = str(len(result.file_results))
    response.headers["X-Field-Ids-Assigned"] = str(result.total_assigned)
    response.headers["X-Field-Ids-Kept"] = str(result.total_kept)
    return response


@app.post("/backfill")
def backfill():
    mapping_text = request.form.get("name_mapping", "")
    source = request.form.get("source", "upload")

    if source == "sample":
        try:
            manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
            uploaded_paths = _sample_uploaded_paths(manifest)
            return _run_and_zip(
                manifest=manifest,
                uploaded_paths=uploaded_paths,
                mapping_text=mapping_text,
            )
        except CheckError as exc:
            return _render(
                error=exc,
                mapping_text=mapping_text,
                source="sample",
                status=400,
            )

    manifest_file = request.files.get("manifest")
    parquet_files = request.files.getlist("files")
    if manifest_file is None or not manifest_file.filename:
        return _render(
            error=CheckError("请上传 Iceberg 有效文件清单（CSV 或 JSON）。"),
            mapping_text=mapping_text,
            source="upload",
            status=400,
        )

    try:
        manifest = parse_manifest(manifest_file.read())
    except CheckError as exc:
        return _render(
            error=exc,
            mapping_text=mapping_text,
            source="upload",
            status=400,
        )

    temp_root = _new_temp_dir()
    try:
        uploaded_paths = _save_uploads(parquet_files, temp_root)
        return _run_and_zip(
            manifest=manifest,
            uploaded_paths=uploaded_paths,
            mapping_text=mapping_text,
        )
    except CheckError as exc:
        return _render(
            error=exc,
            mapping_text=mapping_text,
            source="upload",
            status=400,
        )


@app.errorhandler(413)
def payload_too_large(_error):
    return _render(
        error=CheckError(
            "上传内容超过大小限制 "
            f"（{app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB）。"
        ),
        status=413,
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        debug=bool(os.environ.get("FLASK_DEBUG")),
    )
