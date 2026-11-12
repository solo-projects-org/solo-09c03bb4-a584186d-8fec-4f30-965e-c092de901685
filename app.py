"""Flask front end for Iceberg field-ID backfill (and the legacy checker)."""

from __future__ import annotations

import io
import os
import re
import tempfile
from pathlib import Path

from flask import Flask, Response, render_template, request

from checker.backfill import (
    BackfillResult,
    backfill_field_ids,
    package_zip,
)
from checker.engine import CheckResult, run_check
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.name_mapping import parse_name_mapping

ROOT = Path(__file__).resolve().parent
SAMPLE_DIR = ROOT / "sample"
SAMPLE_TICKETS = "4242, 1003, 4218, 8888, 9999"
SAMPLE_TICKET_COLUMN = "ticket_no"
SAMPLE_MAPPING_FILE = "name-mapping.json"
ZIP_FILENAME = "iceberg-field-ids.zip"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (
    int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024
)
app.config["BACKFILL_BATCH_SIZE"] = int(
    os.environ.get("BACKFILL_BATCH_SIZE", "65536")
)


def parse_tickets(text: str) -> list[int]:
    tokens = [
        token
        for token in re.split(r"[,\s，、;；]+", (text or "").strip())
        if token
    ]
    if not tokens:
        raise CheckError("请输入至少一个整数票号。")
    tickets: list[int] = []
    for token in tokens:
        if not re.fullmatch(r"[+-]?\d+", token):
            raise CheckError(f"票号 “{token}” 不是整数。")
        tickets.append(int(token))
    return tickets


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
    for entry in manifest.required_entries:
        candidate = SAMPLE_DIR / entry.uploaded_filename
        if candidate.is_file():
            paths[entry.uploaded_filename] = candidate
    return paths


# --------------------------------------------------------------------------
# Legacy positional-delete checker (kept available at /sample and /check when
# no name mapping is supplied).
# --------------------------------------------------------------------------


def _render_check(
    *,
    result: CheckResult | None = None,
    error: CheckError | None = None,
    tickets_text: str = "",
    ticket_column: str = "ticket_no",
    source: str = "",
    status: int = 200,
):
    return (
        render_template(
            "index.html",
            result=result,
            error=error,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source=source,
            sample_tickets=SAMPLE_TICKETS,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
            mapping_input="text",
        ),
        status,
    )


@app.get("/checker")
def checker_index():
    return _render_check()


@app.get("/sample")
def sample():
    try:
        manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
        result = run_check(
            manifest,
            _sample_uploaded_paths(manifest),
            parse_tickets(SAMPLE_TICKETS),
            SAMPLE_TICKET_COLUMN,
        )
    except CheckError as exc:
        return _render_check(
            error=exc,
            tickets_text=SAMPLE_TICKETS,
            ticket_column=SAMPLE_TICKET_COLUMN,
            source="sample",
            status=400,
        )
    return _render_check(
        result=result,
        tickets_text=SAMPLE_TICKETS,
        ticket_column=SAMPLE_TICKET_COLUMN,
        source="sample",
    )


def _run_legacy_check(tickets_text: str, ticket_column: str, manifest_file):
    tickets = parse_tickets(tickets_text)
    manifest = parse_manifest(manifest_file.read())
    parquet_files = request.files.getlist("files")
    with tempfile.TemporaryDirectory(prefix="iceberg-pos-delete-") as temp:
        uploaded_paths = _save_uploads(parquet_files, Path(temp))
        return run_check(manifest, uploaded_paths, tickets, ticket_column)


# --------------------------------------------------------------------------
# Field-ID backfill.
# --------------------------------------------------------------------------


def _render_backfill(
    *,
    result: BackfillResult | None = None,
    error: CheckError | None = None,
    manifest_text: str = "",
    mapping_text: str = "",
    source: str = "",
    download_url: str = "",
    status: int = 200,
):
    return (
        render_template(
            "backfill.html",
            result=result,
            error=error,
            manifest_text=manifest_text,
            mapping_text=mapping_text,
            source=source,
            download_url=download_url,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
            batch_size=app.config["BACKFILL_BATCH_SIZE"],
            mapping_input=request.form.get("mapping_input", "text"),
        ),
        status,
    )


def _read_mapping_input() -> bytes | None:
    """Return the name-mapping JSON from an uploaded file or textarea.

    The hidden ``form`` field set by the page tells the server whether the
    textarea or the file is authoritative.  Without it (plain API clients)
    a non-empty textarea wins, then the dedicated file field, then a
    ``.json`` entry in the Parquet multi-select.
    """
    mode = (request.form.get("mapping_input") or "").strip().lower()

    def from_textarea() -> bytes | None:
        for key in ("name_mapping", "mapping", "schema_name_mapping"):
            text = request.form.get(key)
            if text and text.strip():
                return text.encode("utf-8")
        return None

    def from_file() -> bytes | None:
        for key in ("name_mapping_file", "mapping_file", "name_mapping_json"):
            storage = request.files.get(key)
            if storage is not None and storage.filename:
                return storage.read()
        return None

    if mode == "file":
        return from_file() or from_textarea()
    if mode == "text":
        return from_textarea() or from_file()
    return from_textarea() or from_file()


def _collect_replicas(mapping_bytes: bytes | None):
    """Collect Parquet uploads, reading each storage exactly once.

    If no mapping was supplied through a dedicated field, a ``.json`` entry
    in the Parquet multi-select is treated as the name mapping instead.
    """
    replicas = request.files.getlist("files") + request.files.getlist(
        "parquet_files"
    )
    kept = []
    for storage in replicas:
        if storage is None or not storage.filename:
            continue
        name = os.path.basename(storage.filename.replace("\\", "/")).strip()
        lowered = name.lower()
        if lowered.endswith((".json", ".txt")):
            # A mapping file dropped into the Parquet multi-select is never
            # treated as a replica, even when the textarea was also used.
            if mapping_bytes is None:
                mapping_bytes = storage.read()
            continue
        kept.append(storage)
    return kept, mapping_bytes


def _run_backfill(
    manifest_file, mapping_bytes: bytes | None
) -> tuple[BackfillResult, bytes]:
    if manifest_file is None or not manifest_file.filename:
        raise CheckError(
            "请上传 Iceberg 有效文件清单（CSV 或 JSON）。"
        )
    replicas, mapping_bytes = _collect_replicas(mapping_bytes)
    if mapping_bytes is None or not mapping_bytes.strip():
        raise CheckError(
            "请提供表属性 schema.name-mapping.default 的 JSON 内容。"
        )
    manifest = parse_manifest(manifest_file.read())
    mapping = parse_name_mapping(mapping_bytes)
    with tempfile.TemporaryDirectory(prefix="iceberg-field-ids-") as temp:
        uploaded_paths = _save_uploads(replicas, Path(temp))
        result = backfill_field_ids(
            manifest,
            uploaded_paths,
            mapping,
            Path(temp) / "out",
            batch_size=app.config["BACKFILL_BATCH_SIZE"],
        )
        archive = package_zip(result)
    return result, archive


def _zip_response(archive: bytes, filename: str) -> Response:
    return Response(
        archive,
        mimetype="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Field-Id-Backfill": "success",
        },
    )


@app.get("/")
def index():
    return _render_backfill()


@app.post("/backfill")
@app.post("/field-ids")
def backfill():
    manifest_file = request.files.get("manifest")
    mapping_bytes = _read_mapping_input()
    try:
        result, archive = _run_backfill(manifest_file, mapping_bytes)
    except CheckError as exc:
        return _render_backfill(
            error=exc,
            source="upload",
            mapping_text=request.form.get("name_mapping", ""),
            status=400,
        )
    return _zip_response(archive, ZIP_FILENAME)


@app.post("/check")
def check():
    """Legacy checker, or field-ID backfill when a name mapping is supplied."""
    manifest_file = request.files.get("manifest")
    mapping_bytes = _read_mapping_input()
    if mapping_bytes is not None:
        try:
            result, archive = _run_backfill(manifest_file, mapping_bytes)
        except CheckError as exc:
            return _render_backfill(
                error=exc,
                source="upload",
                mapping_text=request.form.get("name_mapping", ""),
                status=400,
            )
        return _zip_response(archive, ZIP_FILENAME)

    tickets_text = request.form.get("tickets", "")
    ticket_column = request.form.get("ticket_column", "ticket_no").strip()
    if manifest_file is None or not manifest_file.filename:
        return _render_check(
            error=CheckError("请上传 Iceberg v2 有效文件清单（CSV 或 JSON）。"),
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )
    try:
        result = _run_legacy_check(tickets_text, ticket_column, manifest_file)
    except CheckError as exc:
        return _render_check(
            error=exc,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )
    return _render_check(
        result=result,
        tickets_text=tickets_text,
        ticket_column=ticket_column,
        source="upload",
    )


def _sample_backfill() -> tuple[BackfillResult, bytes]:
    manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
    mapping = parse_name_mapping(
        (SAMPLE_DIR / SAMPLE_MAPPING_FILE).read_bytes()
    )
    uploads = {
        entry.uploaded_filename: SAMPLE_DIR / entry.uploaded_filename
        for entry in manifest.data_files
    }
    with tempfile.TemporaryDirectory(prefix="iceberg-field-ids-sample-") as temp:
        result = backfill_field_ids(
            manifest,
            uploads,
            mapping,
            Path(temp) / "out",
            batch_size=app.config["BACKFILL_BATCH_SIZE"],
        )
        archive = package_zip(result)
    return result, archive


@app.get("/sample/field-ids")
@app.get("/sample/backfill")
def sample_field_ids():
    try:
        result, _archive = _sample_backfill()
    except CheckError as exc:
        return _render_backfill(
            error=exc,
            source="sample",
            status=400,
        )
    return _render_backfill(
        result=result,
        source="sample",
        download_url="/sample/field-ids.zip",
    )


@app.get("/sample/field-ids.zip")
@app.get("/sample/backfill.zip")
def sample_field_ids_zip():
    try:
        _result, archive = _sample_backfill()
    except CheckError as exc:
        return _render_backfill(
            error=exc,
            source="sample",
            status=400,
        )
    return _zip_response(archive, ZIP_FILENAME)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.errorhandler(413)
def payload_too_large(_error):
    return _render_backfill(
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
