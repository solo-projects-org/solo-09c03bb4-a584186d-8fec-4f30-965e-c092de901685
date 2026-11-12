from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app import app
from checker.engine import existing_field_id
from checker.errors import CheckError

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def _sample_mapping() -> str:
    return (SAMPLE / "name-mapping.json").read_text(encoding="utf-8")


def _sample_uploads():
    files = []
    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
    ):
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    return manifest, files


def test_healthz(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_index_renders(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "字段 ID 回填".encode() in response.data


def test_sample_page_prefills_mapping_and_manifest(client):
    response = client.get("/sample")
    assert response.status_code == 200
    body = response.data.decode()
    assert "deck_fare" in body
    assert "cabin_fare" in body
    assert '"field-id": 21' in body
    assert "data-0001.parquet" in body


def test_backfill_requires_manifest(client):
    response = client.post(
        "/backfill",
        data={"name_mapping": _sample_mapping(), "files": []},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "请上传".encode() in response.data


def test_backfill_requires_mapping(client):
    manifest, files = _sample_uploads()
    response = client.post(
        "/backfill",
        data={"manifest": manifest, "files": files, "name_mapping": ""},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "name-mapping 为空".encode() in response.data


def test_invalid_mapping_json_renders_error_without_download(client):
    manifest, files = _sample_uploads()
    response = client.post(
        "/backfill",
        data={
            "manifest": manifest,
            "files": files,
            "name_mapping": "[{broken",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert response.mimetype != "application/zip"
    assert "JSON 解析失败".encode() in response.data


def test_successful_upload_returns_zip_with_original_names(client):
    manifest, files = _sample_uploads()
    response = client.post(
        "/backfill",
        data={
            "manifest": manifest,
            "files": files,
            "name_mapping": _sample_mapping(),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    disposition = response.headers["Content-Disposition"]
    assert disposition.startswith("attachment;")
    assert "field-ids-backfill.zip" in disposition
    assert response.headers["X-Files-Rewritten"] == "4"
    assigned = int(response.headers["X-Field-Ids-Assigned"])
    assert assigned == 22

    with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
        names = sorted(archive.namelist())
        assert names == sorted(
            [
                "data-0001.parquet",
                "data-0002.parquet",
                "delete-pos-0001.parquet",
                "delete-pos-0002.parquet",
            ]
        )
        with archive.open("data-0001.parquet") as handle:
            schema = pq.read_table(handle).schema
    ids = {
        f.name: existing_field_id(f)
        for f in schema
        if f.name in ("ticket_no", "passenger_name", "fare_note")
    }
    assert ids == {"ticket_no": 1, "passenger_name": 2, "fare_note": 5}


def test_missing_replica_returns_400_and_no_zip(client):
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    only_one = [
        (
            io.BytesIO((SAMPLE / "data-0001.parquet").read_bytes()),
            "data-0001.parquet",
            "application/octet-stream",
        )
    ]
    response = client.post(
        "/backfill",
        data={
            "manifest": manifest,
            "files": only_one,
            "name_mapping": _sample_mapping(),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert response.mimetype != "application/zip"
    body = response.data.decode()
    assert "上传副本缺失" in body
    assert "data-0002.parquet" in body


def test_ambiguous_column_error_names_file_and_column(client, tmp_path):
    # Build a one-file manifest + parquet with an ambiguous top-level match.
    import pyarrow as pa

    parquet_path = tmp_path / "dup.parquet"
    table = pa.Table.from_pylist(
        [{"shared": 1}],
        schema=pa.schema([pa.field("shared", pa.int64())]),
    )
    pq.write_table(table, parquet_path)
    manifest_csv = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/dup.parquet,dup.parquet,data,1\n"
    )
    mapping = (
        '[{"field-id": 1, "names": ["shared"]},'
        ' {"field-id": 2, "names": ["shared", "other"]}]'
    )
    response = client.post(
        "/backfill",
        data={
            "manifest": (io.BytesIO(manifest_csv.encode()), "manifest.csv"),
            "files": [
                (
                    io.BytesIO(parquet_path.read_bytes()),
                    "dup.parquet",
                    "application/octet-stream",
                )
            ],
            "name_mapping": mapping,
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    body = response.data.decode()
    assert "dup.parquet" in body
    assert "shared" in body
    assert "回填未执行" in body


def test_sample_post_returns_zip(client):
    response = client.post(
        "/backfill",
        data={"source": "sample", "name_mapping": _sample_mapping()},
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
        assert "data-0001.parquet" in archive.namelist()
        # The two struct amount columns get distinct IDs in the ZIP output.
        with archive.open("data-0001.parquet") as handle:
            schema = pq.ParquetFile(handle).schema_arrow
    deck = schema.field("deck_fare").type.field("amount")
    cabin = schema.field("cabin_fare").type.field("amount")
    assert deck.metadata[b"PARQUET:field_id"] == b"21"
    assert cabin.metadata[b"PARQUET:field_id"] == b"31"


def test_sample_post_with_broken_mapping_renders_error(client):
    response = client.post(
        "/backfill",
        data={"source": "sample", "name_mapping": "{}"},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert response.mimetype != "application/zip"


def test_unused_uploads_do_not_break_batch(client):
    manifest, files = _sample_uploads()
    files.append(
        (
            io.BytesIO(b"extra"),
            "not-in-manifest.parquet",
            "application/octet-stream",
        )
    )
    response = client.post(
        "/backfill",
        data={
            "manifest": manifest,
            "files": files,
            "name_mapping": _sample_mapping(),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
        assert "not-in-manifest.parquet" not in archive.namelist()


def test_check_error_carries_files_and_columns():
    error = CheckError("boom", files=["a.parquet"], columns=["s.amount"])
    assert error.files == ["a.parquet"]
    assert error.columns == ["s.amount"]
    assert "a.parquet" in str(error)
