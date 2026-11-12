from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.backfill import backfill_field_ids, package_zip, resolve_schema_ids
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.name_mapping import parse_name_mapping

SAMPLE = Path(__file__).resolve().parents[1] / "sample"
FIELD_ID_KEY = b"PARQUET:field_id"

MAPPING = [
    {"field-id": 10, "names": ["ticket_no"]},
    {
        "field-id": 20,
        "names": ["base_fare"],
        "fields": [
            {"field-id": 21, "names": ["fare_cents"]},
            {"field-id": 22, "names": ["currency"]},
        ],
    },
    {
        "field-id": 30,
        "names": ["surcharge"],
        "fields": [
            {"field-id": 31, "names": ["fare_cents"]},
            {"field-id": 32, "names": ["currency"]},
        ],
    },
]


def field_id(field: pa.Field) -> int | None:
    metadata = field.metadata or {}
    value = metadata.get(FIELD_ID_KEY)
    return int(value) if value is not None else None


def child_ids(field: pa.Field) -> dict[str, int | None]:
    return {child.name: field_id(child) for child in field.type}


def write_struct_file(path: Path, *, parent_ids: bool = False) -> None:
    schema = pa.schema(
        [
            pa.field(
                "ticket_no",
                pa.int64(),
                metadata={FIELD_ID_KEY: b"10"} if parent_ids else None,
            ),
            pa.field(
                "base_fare",
                pa.struct(
                    [("fare_cents", pa.int64()), ("currency", pa.string())]
                ),
                metadata={FIELD_ID_KEY: b"20"} if parent_ids else None,
            ),
            pa.field(
                "surcharge",
                pa.struct(
                    [("fare_cents", pa.int64()), ("currency", pa.string())]
                ),
                metadata={FIELD_ID_KEY: b"30"} if parent_ids else None,
            ),
        ]
    )
    table = pa.Table.from_pydict(
        {
            "ticket_no": [100, 200, 300],
            "base_fare": [
                {"fare_cents": 1500, "currency": "CNY"},
                {"fare_cents": 1800, "currency": "CNY"},
                {"fare_cents": 900, "currency": "CNY"},
            ],
            "surcharge": [
                {"fare_cents": 200, "currency": "CNY"},
                {"fare_cents": 150, "currency": "CNY"},
                {"fare_cents": 80, "currency": "CNY"},
            ],
        },
        schema=schema,
    )
    pq.write_table(table, path, row_group_size=1, compression="snappy")


def manifest_for(paths: list[tuple[str, Path]]) -> str:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number"
    ]
    for index, (original, path) in enumerate(paths, start=1):
        lines.append(f"{original},{path.name},data,{index}")
    return "\n".join(lines) + "\n"


def read_ids(path: Path) -> pa.Schema:
    return pq.ParquetFile(str(path)).schema_arrow


def test_resolve_schema_fills_ids_through_parent_id():
    schema = pa.schema(
        [
            pa.field("ticket_no", pa.int64()),
            pa.field(
                "base_fare",
                pa.struct([("fare_cents", pa.int64()), ("currency", pa.string())]),
            ),
            pa.field(
                "surcharge",
                pa.struct([("fare_cents", pa.int64()), ("currency", pa.string())]),
            ),
        ]
    )
    target, changes = resolve_schema_ids(
        schema, parse_name_mapping(json.dumps(MAPPING)), file_label="f.parquet"
    )
    assert field_id(target.field("ticket_no")) == 10
    assert field_id(target.field("base_fare")) == 20
    assert child_ids(target.field("base_fare")) == {
        "fare_cents": 21,
        "currency": 22,
    }
    assert field_id(target.field("surcharge")) == 30
    # The *same* child name gets a different ID under a different parent.
    assert child_ids(target.field("surcharge")) == {
        "fare_cents": 31,
        "currency": 32,
    }
    assert [c.path for c in changes] == [
        "ticket_no",
        "base_fare",
        "base_fare.fare_cents",
        "base_fare.currency",
        "surcharge",
        "surcharge.fare_cents",
        "surcharge.currency",
    ]


def test_existing_ids_are_never_overwritten():
    schema = pa.schema(
        [
            pa.field(
                "ticket_no",
                pa.int64(),
                metadata={FIELD_ID_KEY: b"99"},
            ),
            pa.field(
                "base_fare",
                pa.struct([("fare_cents", pa.int64())]),
                metadata={FIELD_ID_KEY: b"20"},
            ),
        ]
    )
    target, changes = resolve_schema_ids(
        schema, parse_name_mapping(json.dumps(MAPPING)), file_label="f.parquet"
    )
    assert field_id(target.field("ticket_no")) == 99
    # Existing ID wins even though the mapping says 10.
    assert child_ids(target.field("base_fare")) == {"fare_cents": 21}
    assert [c.added for c in changes] == [False, False, True]


def test_child_without_usable_parent_id_is_rejected():
    # The parent is present but has no ID, so the child cannot locate its
    # mapping branch even though "fare_cents" exists somewhere in the mapping.
    schema = pa.schema(
        [
            pa.field(
                "base_fare",
                pa.struct([("fare_cents", pa.int64())]),
                metadata={FIELD_ID_KEY: b"999"},
            ),
        ]
    )
    with pytest.raises(CheckError) as excinfo:
        resolve_schema_ids(
            schema, parse_name_mapping(json.dumps(MAPPING)), file_label="f.parquet"
        )
    assert "fare_cents" in excinfo.value.message
    assert "父字段" in excinfo.value.message
    assert excinfo.value.columns == ["base_fare.fare_cents"]


def test_ambiguous_aliases_are_rejected():
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 1, "names": ["amount"]},
                {"field-id": 2, "names": ["amount"]},
            ]
        )
    )
    schema = pa.schema([pa.field("amount", pa.int64())])
    with pytest.raises(CheckError, match="多个"):
        resolve_schema_ids(schema, mapping, file_label="f.parquet")


def test_unmatched_column_is_rejected():
    mapping = parse_name_mapping(json.dumps(MAPPING))
    schema = pa.schema([pa.field("mystery", pa.string())])
    with pytest.raises(CheckError) as excinfo:
        resolve_schema_ids(schema, mapping, file_label="f.parquet")
    assert "mystery" in excinfo.value.message
    assert "mystery" in excinfo.value.columns


def test_duplicate_existing_id_is_rejected():
    schema = pa.schema(
        [
            pa.field("a", pa.int64(), metadata={FIELD_ID_KEY: b"5"}),
            pa.field("b", pa.int64(), metadata={FIELD_ID_KEY: b"5"}),
        ]
    )
    mapping = parse_name_mapping(json.dumps([{"field-id": 5, "names": ["a"]}]))
    # ``b`` has an ID that is not in the mapping; it is still scanned for
    # duplicate IDs.
    with pytest.raises(CheckError) as excinfo:
        resolve_schema_ids(schema, mapping, file_label="f.parquet")
    assert "重复" in excinfo.value.message
    assert "a" in excinfo.value.columns and "b" in excinfo.value.columns


def test_new_id_clashing_with_existing_id_is_rejected():
    schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.int64(), metadata={FIELD_ID_KEY: b"21"}),
        ]
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 21, "names": ["a"]},
                {"field-id": 7, "names": ["b"]},
            ]
        )
    )
    with pytest.raises(CheckError, match="重复"):
        resolve_schema_ids(schema, mapping, file_label="f.parquet")


def test_nested_struct_beyond_one_level_is_rejected():
    schema = pa.schema(
        [pa.field("outer", pa.struct([("inner", pa.struct([("x", pa.int64())]))]))]
    )
    mapping = parse_name_mapping(json.dumps([{"field-id": 1, "names": ["outer"]}]))
    with pytest.raises(CheckError, match="只支持标量列和一层 struct"):
        resolve_schema_ids(schema, mapping, file_label="f.parquet")


def test_list_column_is_rejected():
    schema = pa.schema([pa.field("tags", pa.list_(pa.string()))])
    mapping = parse_name_mapping(json.dumps([{"field-id": 1, "names": ["tags"]}]))
    with pytest.raises(CheckError, match="只支持标量列和一层 struct"):
        resolve_schema_ids(schema, mapping, file_label="f.parquet")


def test_backfill_rewrites_data_files_and_packages_original_names(tmp_path: Path):
    source = tmp_path / "data-0001.parquet"
    write_struct_file(source)
    manifest = parse_manifest(
        manifest_for([("s3://bucket/data/data-0001.parquet", source)])
    )
    mapping = parse_name_mapping(json.dumps(MAPPING))

    result = backfill_field_ids(
        manifest, {"data-0001.parquet": source}, mapping, tmp_path / "out"
    )
    assert len(result.files) == 1
    assert result.added_count == 3 + 4  # three top level columns, four children
    output = result.files[0].output_path
    oracle_ids = field_id_from_schema(read_ids(output))
    assert oracle_ids["base_fare"] == 20
    assert oracle_ids["surcharge"] == 30
    assert oracle_ids["base_fare.fare_cents"] == 21
    assert oracle_ids["surcharge.fare_cents"] == 31

    # Values, types, column names and row order are unchanged.
    before = pq.read_table(str(source)).to_pydict()
    after = pq.read_table(str(output)).to_pydict()
    assert before == after
    assert pq.ParquetFile(str(output)).metadata.num_rows == (
        pq.ParquetFile(str(source)).metadata.num_rows
    )

    archive = package_zip(result)
    names = zipfile.ZipFile(io.BytesIO(archive)).namelist()
    assert names == ["data-0001.parquet"]


def field_id_from_schema(schema: pa.Schema) -> dict[str, int]:
    result: dict[str, int] = {}

    def visit(field: pa.Field, path: str) -> None:
        value = field_id(field)
        if value is not None:
            result[path] = value
        if pa.types.is_struct(field.type):
            for child in field.type:
                visit(child, f"{path}.{child.name}")

    for field in schema:
        visit(field, field.name)
    return result


def test_backfill_keeps_existing_ids_and_preserves_parquet_schema_ids(tmp_path: Path):
    source = tmp_path / "nested.parquet"
    schema = pa.schema(
        [
            pa.field(
                "ticket_no",
                pa.int64(),
                metadata={FIELD_ID_KEY: b"10"},
            ),
            pa.field(
                "base_fare",
                pa.struct(
                    [("fare_cents", pa.int64()), ("currency", pa.string())]
                ),
                metadata={FIELD_ID_KEY: b"20"},
            ),
        ]
    )
    table = pa.Table.from_pydict(
        {"ticket_no": [1], "base_fare": [{"fare_cents": 5, "currency": "CNY"}]},
        schema=schema,
    )
    pq.write_table(table, source)
    manifest = parse_manifest(
        manifest_for([("s3://bucket/data/nested.parquet", source)])
    )

    result = backfill_field_ids(
        manifest,
        {"nested.parquet": source},
        parse_name_mapping(json.dumps(MAPPING)),
        tmp_path / "out",
    )
    written = pq.ParquetFile(str(result.files[0].output_path))
    parquet_schema = str(written.schema)
    assert "field_id=10 ticket_no" in parquet_schema
    assert "field_id=20 base_fare" in parquet_schema
    assert "field_id=21 fare_cents" in parquet_schema
    assert "field_id=22 currency" in parquet_schema
    assert result.kept_count == 2


def test_missing_data_replica_aborts_without_output(tmp_path: Path):
    source = tmp_path / "present.parquet"
    write_struct_file(source)
    missing = tmp_path / "missing.parquet"
    manifest = parse_manifest(
        manifest_for(
            [
                ("s3://bucket/data/present.parquet", source),
                ("s3://bucket/data/missing.parquet", missing),
            ]
        )
    )
    out = tmp_path / "out"
    with pytest.raises(CheckError) as excinfo:
        backfill_field_ids(
            manifest,
            {"present.parquet": source},
            parse_name_mapping(json.dumps(MAPPING)),
            out,
        )
    assert "missing.parquet" in excinfo.value.files
    assert "缺失" in excinfo.value.message
    assert not out.exists() or not any(out.iterdir())


def test_unmatched_column_aborts_for_whole_batch(tmp_path: Path):
    good = tmp_path / "good.parquet"
    write_struct_file(good)
    bad = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"unknown_column": [1]}), bad)
    manifest = parse_manifest(
        manifest_for(
            [
                ("s3://bucket/data/good.parquet", good),
                ("s3://bucket/data/bad.parquet", bad),
            ]
        )
    )
    out = tmp_path / "out"
    with pytest.raises(CheckError) as excinfo:
        backfill_field_ids(
            manifest,
            {"good.parquet": good, "bad.parquet": bad},
            parse_name_mapping(json.dumps(MAPPING)),
            out,
        )
    assert excinfo.value.files == ["bad.parquet"]
    assert "unknown_column" in excinfo.value.columns
    assert not any(out.iterdir()) if out.exists() else True


def test_corrupt_data_file_aborts_with_file_and_no_output(tmp_path: Path):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    manifest = parse_manifest(manifest_for([("s3://bucket/data/bad.parquet", bad)]))
    with pytest.raises(CheckError) as excinfo:
        backfill_field_ids(
            manifest,
            {"bad.parquet": bad},
            parse_name_mapping(json.dumps(MAPPING)),
            tmp_path / "out",
        )
    assert "bad.parquet" in excinfo.value.files
    assert "无法读取数据文件" in excinfo.value.message


def test_manifest_without_data_files_is_rejected(tmp_path: Path):
    manifest = parse_manifest(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/delete/d.parquet,d.parquet,position_delete,3\n"
    )
    with pytest.raises(CheckError, match="没有 data"):
        backfill_field_ids(
            manifest, {}, parse_name_mapping(json.dumps(MAPPING)), tmp_path / "out"
        )


def test_unused_uploads_are_reported_as_warning(tmp_path: Path):
    source = tmp_path / "data.parquet"
    write_struct_file(source)
    other = tmp_path / "ignored.parquet"
    write_struct_file(other)
    manifest = parse_manifest(
        manifest_for([("s3://bucket/data/data.parquet", source)])
    )
    result = backfill_field_ids(
        manifest,
        {"data.parquet": source, "ignored.parquet": other},
        parse_name_mapping(json.dumps(MAPPING)),
        tmp_path / "out",
    )
    assert result.warnings
    assert "ignored.parquet" in result.warnings[0]
    assert "ignored.parquet" not in [
        item.entry.uploaded_filename for item in result.files
    ]


def test_sample_backfill_uses_parent_ids_for_same_named_children(
    tmp_path: Path,
):
    """The bundled sample has fare_cents under two different structs."""
    manifest = parse_manifest((SAMPLE / "manifest.csv").read_bytes())
    mapping = parse_name_mapping((SAMPLE / "name-mapping.json").read_bytes())
    uploads = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.data_files
    }
    result = backfill_field_ids(
        manifest, uploads, mapping, tmp_path / "out"
    )
    assert len(result.files) == 2
    for item in result.files:
        ids = field_id_from_schema(read_ids(item.output_path))
        assert ids["base_fare.fare_cents"] == 31
        assert ids["surcharge.fare_cents"] == 41
        # The two same-named old amount columns now have distinct IDs.
        assert ids["base_fare.fare_cents"] != ids["surcharge.fare_cents"]


def test_row_group_count_and_order_are_preserved(tmp_path: Path):
    source = tmp_path / "grouped.parquet"
    write_struct_file(source)
    manifest = parse_manifest(
        manifest_for([("s3://bucket/data/grouped.parquet", source)])
    )
    result = backfill_field_ids(
        manifest,
        {"grouped.parquet": source},
        parse_name_mapping(json.dumps(MAPPING)),
        tmp_path / "out",
        batch_size=1,
    )
    original = pq.ParquetFile(str(source))
    rewritten = pq.ParquetFile(str(result.files[0].output_path))
    assert original.metadata.num_rows == rewritten.metadata.num_rows == 3
    assert original.read().to_pydict() == rewritten.read().to_pydict()
