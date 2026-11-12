from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app import app
from checker.backfill import FIELD_ID_METADATA_KEY

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def upload_form(*, with_mapping=True, files=None):
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    data = {
        "manifest": manifest,
        "files": files
        if files is not None
        else [
            (
                io.BytesIO((SAMPLE / "data-0001.parquet").read_bytes()),
                "data-0001.parquet",
                "application/octet-stream",
            ),
            (
                io.BytesIO((SAMPLE / "data-0002.parquet").read_bytes()),
                "data-0002.parquet",
                "application/octet-stream",
            ),
        ],
    }
    if with_mapping:
        data["name_mapping"] = (SAMPLE / "name-mapping.json").read_text(
            encoding="utf-8"
        )
    return data


def field_id(field) -> int | None:
    value = (field.metadata or {}).get(FIELD_ID_METADATA_KEY)
    return int(value) if value is not None else None


def ids_from_zip(payload: bytes) -> dict[str, dict[str, int]]:
    result = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            data = archive.read(name)
            path = Path("/tmp") / f"check-{name}"
            path.write_bytes(data)
            schema = pq.ParquetFile(str(path)).schema_arrow
            ids = {}
            for item in schema:
                if field_id(item) is not None:
                    ids[item.name] = field_id(item)
                if str(item.type).startswith("struct"):
                    for child in item.type:
                        if field_id(child) is not None:
                            ids[f"{item.name}.{child.name}"] = field_id(child)
            result[name] = ids
            path.unlink()
    return result


def test_index_shows_backfill_form(client):
    response = client.get("/")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "字段 ID 回填" in html
    assert "name-mapping" in html or "name_mapping" in html


def test_sample_page_shows_two_same_named_children_with_distinct_ids(client):
    response = client.get("/sample/field-ids")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "base_fare.fare_cents" in html
    assert "surcharge.fare_cents" in html
    assert ">31<" in html
    assert ">41<" in html
    assert "下载 ZIP" in html


def test_sample_zip_contains_rewritten_data_files_only(client):
    response = client.get("/sample/field-ids.zip")
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    assert "attachment" in response.headers["Content-Disposition"]
    ids = ids_from_zip(response.data)
    assert sorted(ids) == ["data-0001.parquet", "data-0002.parquet"]
    for name in ids:
        assert ids[name]["base_fare.fare_cents"] == 31
        assert ids[name]["surcharge.fare_cents"] == 41
        assert ids[name]["ticket_no"] == 1


def test_upload_backfill_returns_zip_named_after_uploads(client):
    response = client.post(
        "/backfill",
        data=upload_form(),
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
        assert archive.namelist() == ["data-0001.parquet", "data-0002.parquet"]
        rewritten = pq.ParquetFile(
            io.BytesIO(archive.read("data-0001.parquet"))
        )
    text = str(rewritten.schema)
    assert "field_id=31 fare_cents" in text
    assert "field_id=41 fare_cents" in text


def test_json_mapping_may_be_uploaded_as_a_file(client):
    mapping = (
        io.BytesIO((SAMPLE / "name-mapping.json").read_bytes()),
        "name-mapping.json",
        "application/json",
    )
    data = upload_form(with_mapping=False)
    data["name_mapping_file"] = mapping
    response = client.post(
        "/backfill",
        data=data,
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.mimetype == "application/zip"


def test_missing_mapping_is_rejected_without_download(client):
    response = client.post(
        "/backfill",
        data=upload_form(with_mapping=False),
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "name-mapping.default" in html or "name mapping" in html
    assert "没有提供下载" in html


def test_missing_parquet_replica_names_file_and_column(client):
    data = upload_form()
    data["files"] = [data["files"][0]]  # only data-0001 uploaded
    response = client.post(
        "/backfill",
        data=data,
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "data-0002.parquet" in html
    assert "本批未完成" in html
    assert "没有提供下载" in html
    assert "下载 ZIP" not in html


def test_unmatched_column_names_file_and_column(client):
    import pyarrow as pa
    import pyarrow.parquet as pq

    bad = io.BytesIO()
    pq.write_table(pa.table({"surprise": [1]}), bad)
    bad.seek(0)
    response = client.post(
        "/backfill",
        data=upload_form(
            files=[
                (
                    io.BytesIO((SAMPLE / "data-0001.parquet").read_bytes()),
                    "data-0001.parquet",
                    "application/octet-stream",
                ),
                (bad, "data-0002.parquet", "application/octet-stream"),
            ]
        ),
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "data-0002.parquet" in html
    assert "surprise" in html
    assert "涉及列" in html
    assert "没有提供下载" in html


def test_returns_zip_on_success(client):
    response = client.post(
        "/backfill",
        data=upload_form(),
        content_type="multipart/form-data",
    )
    assert response.status_code == 200


def test_duplicate_fields_within_file_name_file_and_columns(client):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64(), metadata={b"PARQUET:field_id": b"7"}),
            pa.field(
                "passenger_name",
                pa.string(),
                metadata={b"PARQUET:field_id": b"7"},
            ),
        ]
    )
    payload = io.BytesIO()
    pq.write_table(pa.table(
        {"ticket_no": [1], "passenger_name": ["P"]}, schema=schema
    ), payload)
    # The same (duplicate) replica is supplied for both manifest data files,
    # so the missing-file check passes and the duplicate-ID check is reached.
    # Each upload needs its own buffer object.
    first = io.BytesIO(payload.getvalue())
    second = io.BytesIO(payload.getvalue())
    response = client.post(
        "/backfill",
        data=upload_form(
            files=[
                (first, "data-0001.parquet", "application/octet-stream"),
                (second, "data-0002.parquet", "application/octet-stream"),
            ]
        ),
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "data-0001.parquet" in html
    assert "重复" in html
    assert "ticket_no" in html and "passenger_name" in html
    assert "没有提供下载" in html


def test_bad_mapping_json_is_rejected(client):
    data = upload_form()
    data["name_mapping"] = "{not json"
    response = client.post(
        "/backfill",
        data=data,
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "不是有效的 JSON" in response.get_data(as_text=True)


def test_health(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_legacy_checker_still_served(client):
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    files = []
    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ):
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": files,
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "核对结果" in html
    assert "delete-pos-0001.parquet" in html
