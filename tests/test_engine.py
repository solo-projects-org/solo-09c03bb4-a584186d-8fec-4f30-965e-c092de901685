from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.engine import existing_field_id, run_backfill
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.namemapping import parse_name_mapping

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def write_parquet(path: Path, fields: list[pa.Field], rows: list[dict], **kwargs):
    schema = pa.schema(fields)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, **kwargs)


def field_ids(schema: pa.Schema) -> dict[str, int | None]:
    found: dict[str, int | None] = {}

    def walk(type_: pa.DataType, prefix: str = ""):
        for child in type_:
            path = f"{prefix}{child.name}"
            found[path] = existing_field_id(child)
            if pa.types.is_struct(child.type):
                walk(child.type, path + ".")

    walk(schema)
    return found


def make_manifest(tmp_path: Path, entries: list[tuple[str, str, str, int]]):
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number"
    ]
    for original, uploaded, kind, seq in entries:
        lines.append(f"{original},{uploaded},{kind},{seq}")
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return parse_manifest(manifest_path.read_bytes())


def sample_mapping() -> str:
    return (SAMPLE / "name-mapping.json").read_text(encoding="utf-8")


def sample_manifest():
    return parse_manifest((SAMPLE / "manifest.csv").read_bytes())


# --------------------------------------------------------------------------- #
# Field-ID extraction
# --------------------------------------------------------------------------- #


def test_existing_field_id_reads_valid_metadata():
    field = pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"7"})
    assert existing_field_id(field) == 7


@pytest.mark.parametrize("raw", [b"abc", b"-1", b""])
def test_existing_field_id_treats_invalid_and_sentinel_as_missing(raw):
    field = pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": raw})
    assert existing_field_id(field) is None


def test_existing_field_id_without_metadata_is_none():
    assert existing_field_id(pa.field("a", pa.int64())) is None


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #


def test_sample_batch_assigns_scoped_struct_ids_and_preserves_everything(tmp_path):
    manifest = sample_manifest()
    mapping = parse_name_mapping(sample_mapping())
    paths = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    out = tmp_path / "out"
    result = run_backfill(manifest, paths, mapping, out)

    # Every required replica is rewritten under its original uploaded name.
    assert sorted(fr.entry.uploaded_filename for fr in result.file_results) == [
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
    ]

    data1 = pq.ParquetFile(out / "data-0001.parquet")
    ids = field_ids(data1.schema_arrow)
    assert ids["ticket_no"] == 1  # pre-existing ID is kept
    assert ids["passenger_name"] == 2
    assert ids["deck_fare"] == 20
    assert ids["deck_fare.amount"] == 21
    assert ids["deck_fare.currency"] == 22
    assert ids["cabin_fare"] == 30
    # Same leaf name "amount" must resolve to a different ID via parent scope.
    assert ids["cabin_fare.amount"] == 31
    assert ids["cabin_fare.currency"] == 32
    assert ids["fare_note"] == 5  # matched through the "remark" alias
    assert ids["voyage_date"] == 6

    delete1 = pq.ParquetFile(out / "delete-pos-0001.parquet")
    delete_ids = field_ids(delete1.schema_arrow)
    assert delete_ids == {"file_path": 41, "pos": 42}


def test_rewrite_preserves_values_row_order_types_and_row_groups(tmp_path):
    manifest = sample_manifest()
    mapping = parse_name_mapping(sample_mapping())
    paths = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    out = tmp_path / "out"
    run_backfill(manifest, paths, mapping, out)

    for entry in manifest.required_entries:
        name = entry.uploaded_filename
        before = pq.ParquetFile(SAMPLE / name)
        after = pq.ParquetFile(out / name)
        assert after.num_row_groups == before.num_row_groups
        assert after.metadata.num_rows == before.metadata.num_rows
        source_table = pq.read_table(SAMPLE / name)
        output_table = pq.read_table(out / name)
        assert output_table.equals(source_table)
        # Types and column order/names survive.
        assert [f.name for f in output_table.schema] == [
            f.name for f in source_table.schema
        ]
        assert [f.type for f in output_table.schema] == [
            f.type for f in source_table.schema
        ]


def test_rewrite_preserves_compression_and_statistics(tmp_path):
    manifest = sample_manifest()
    mapping = parse_name_mapping(sample_mapping())
    paths = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    out = tmp_path / "out"
    run_backfill(manifest, paths, mapping, out)

    name = "data-0002.parquet"  # mixed compression + statistics disabled
    before = pq.ParquetFile(SAMPLE / name).metadata
    after = pq.ParquetFile(out / name).metadata
    assert after.num_columns == before.num_columns
    for index in range(before.num_columns):
        c1 = before.row_group(0).column(index)
        c2 = after.row_group(0).column(index)
        assert c2.path_in_schema == c1.path_in_schema
        assert c2.compression == c1.compression
        assert c2.is_stats_set == c1.is_stats_set


def test_thrift_schema_carries_field_ids_for_struct_children(tmp_path):
    manifest = sample_manifest()
    mapping = parse_name_mapping(sample_mapping())
    paths = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    out = tmp_path / "out"
    run_backfill(manifest, paths, mapping, out)
    raw = pq.ParquetFile(out / "data-0001.parquet").schema
    text = str(raw)
    assert "field_id=20 deck_fare" in text
    assert "field_id=21 amount" in text
    assert "field_id=31 amount" in text


def test_schema_kv_metadata_and_nullability_are_preserved(tmp_path):
    inner = pa.struct(
        [
            pa.field("amount", pa.int64(), nullable=False),
            pa.field("currency", pa.string()),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("fare", inner),
        ],
        metadata={b"iceberg.schema": b'{"schema-id":3}', b"custom.kv": b"v1"},
    )
    rows = [
        {"id": i, "fare": {"amount": 100 + i, "currency": "CNY"}}
        for i in range(4)
    ]
    source = tmp_path / "a.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema), source, row_group_size=2
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 1, "names": ["id"]},
                {
                    "field-id": 10,
                    "names": ["fare"],
                    "fields": [
                        {"field-id": 11, "names": ["amount"]},
                        {"field-id": 12, "names": ["currency"]},
                    ],
                },
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    out = tmp_path / "out"
    run_backfill(manifest, {"a.parquet": source}, mapping, out)

    rewritten = pq.ParquetFile(out / "a.parquet")
    new_schema = rewritten.schema_arrow
    assert new_schema.metadata == schema.metadata
    assert new_schema.field("id").nullable is False
    fare = new_schema.field("fare")
    assert fare.type.field("amount").nullable is False
    assert fare.type.field("currency").nullable is True
    assert rewritten.num_row_groups == 2
    assert pq.read_table(out / "a.parquet").column("id").to_pylist() == [
        0,
        1,
        2,
        3,
    ]


def test_backfill_is_idempotent(tmp_path):
    manifest = sample_manifest()
    mapping = parse_name_mapping(sample_mapping())
    paths = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    first = tmp_path / "first"
    result1 = run_backfill(manifest, paths, mapping, first)
    assert result1.total_assigned > 0

    second_paths = {
        entry.uploaded_filename: first / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    second = tmp_path / "second"
    result2 = run_backfill(manifest, second_paths, mapping, second)
    assert result2.total_assigned == 0
    assert result2.total_kept == result1.total_kept + result1.total_assigned
    for entry in manifest.required_entries:
        name = entry.uploaded_filename
        assert pq.read_table(first / name).equals(pq.read_table(second / name))


# --------------------------------------------------------------------------- #
# Scoped matching rules
# --------------------------------------------------------------------------- #


def test_struct_child_uses_parent_scope_not_global_names(tmp_path):
    # Two structs each with "amount"; only the parent-scoped mapping exists.
    # A global mapping containing "amount" must not be consulted.
    fields = [
        pa.field("id", pa.int64()),
        pa.field(
            "deck_fare",
            pa.struct([pa.field("amount", pa.int64())]),
        ),
        pa.field(
            "cabin_fare",
            pa.struct([pa.field("amount", pa.int64())]),
        ),
    ]
    rows = [
        {"id": 1, "deck_fare": {"amount": 10}, "cabin_fare": {"amount": 20}}
    ]
    source = tmp_path / "a.parquet"
    write_parquet(source, fields, rows)
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 1, "names": ["id"]},
                {
                    "field-id": 20,
                    "names": ["deck_fare"],
                    "fields": [{"field-id": 21, "names": ["amount"]}],
                },
                {
                    "field-id": 30,
                    "names": ["cabin_fare"],
                    "fields": [{"field-id": 31, "names": ["amount"]}],
                },
            ]
        )
    )
    manifest = make_manifest(
        tmp_path,
        [("s3://b/a.parquet", "a.parquet", "data", 1)],
    )
    out = tmp_path / "out"
    run_backfill(manifest, {"a.parquet": source}, mapping, out)
    ids = field_ids(pq.ParquetFile(out / "a.parquet").schema_arrow)
    assert ids["deck_fare.amount"] == 21
    assert ids["cabin_fare.amount"] == 31


def test_alias_matches_when_parquet_name_differs(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [pa.field("remark", pa.string())],
        [{"remark": "x"}],
    )
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 5, "names": ["fare_note", "remark"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    out = tmp_path / "out"
    result = run_backfill(manifest, {"a.parquet": source}, mapping, out)
    assert result.file_results[0].changes[0].field_id == 5
    assert pq.ParquetFile(out / "a.parquet").schema_arrow.field(
        "remark"
    ).metadata[b"PARQUET:field_id"] == b"5"


def test_unmatched_column_names_file_and_column(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(source, [pa.field("ghost", pa.string())], [{"ghost": "x"}])
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["other"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError) as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.files == ["a.parquet"]
    assert exc_info.value.columns == ["ghost"]
    assert not (tmp_path / "out").exists() or not any(
        (tmp_path / "out").iterdir()
    )


def test_ambiguous_top_level_match_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(source, [pa.field("shared", pa.int64())], [{"shared": 1}])
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 1, "names": ["shared", "one"]},
                {"field-id": 2, "names": ["two", "shared"]},
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="多个字段 ID") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.files == ["a.parquet"]
    assert exc_info.value.columns == ["shared"]


def test_ambiguous_struct_child_match_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field(
                "deck_fare",
                pa.struct([pa.field("amount", pa.int64())]),
                metadata={b"PARQUET:field_id": b"20"},
            )
        ],
        [{"deck_fare": {"amount": 1}}],
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {
                    "field-id": 20,
                    "names": ["deck_fare"],
                    "fields": [
                        {"field-id": 21, "names": ["amount"]},
                        {"field-id": 23, "names": ["price", "amount"]},
                    ],
                }
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="多个字段 ID") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.columns == ["deck_fare.amount"]


def test_struct_child_without_parent_submapping_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field(
                "deck_fare",
                pa.struct([pa.field("amount", pa.int64())]),
                metadata={b"PARQUET:field_id": b"20"},
            )
        ],
        [{"deck_fare": {"amount": 1}}],
    )
    # Parent node 20 exists but has no children at all.
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 20, "names": ["deck_fare"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="子映射") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.columns == ["deck_fare.amount"]


# --------------------------------------------------------------------------- #
# Duplicate IDs, unsupported types, missing files
# --------------------------------------------------------------------------- #


def test_duplicate_id_within_one_file_is_rejected(tmp_path):
    # Two columns already carrying the same field ID must be rejected even
    # though neither column needs a mapping lookup.
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"9"}),
            pa.field("b", pa.int64(), metadata={b"PARQUET:field_id": b"9"}),
        ],
        [{"a": 1, "b": 2}],
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 9, "names": ["a"]},
                {"field-id": 10, "names": ["b"]},
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="字段 ID 9 同时出现") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert set(exc_info.value.columns) == {"a", "b"}


def test_mapped_id_colliding_with_existing_in_file_id_is_rejected(tmp_path):
    # Column a already carries ID 7; column b is unmapped in the file but its
    # mapping resolves to 7, producing an in-file duplicate.
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field("a", pa.int64(), metadata={b"PARQUET:field_id": b"7"}),
            pa.field("b", pa.int64()),
        ],
        [{"a": 1, "b": 2}],
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 9, "names": ["a"]},
                {"field-id": 7, "names": ["b"]},
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="字段 ID 7 同时出现"):
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )


def test_nested_struct_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field(
                "outer",
                pa.struct(
                    [
                        pa.field(
                            "inner",
                            pa.struct([pa.field("x", pa.int64())]),
                        )
                    ]
                ),
            )
        ],
        [{"outer": {"inner": {"x": 1}}}],
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {
                    "field-id": 1,
                    "names": ["outer"],
                    "fields": [
                        {
                            "field-id": 2,
                            "names": ["inner"],
                            "fields": [{"field-id": 3, "names": ["x"]}],
                        }
                    ],
                }
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="嵌套 struct") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.columns == ["outer.inner"]


def test_list_column_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [pa.field("tags", pa.list_(pa.int64()))],
        [{"tags": [1, 2]}],
    )
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["tags"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="list / map") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.columns == ["tags"]


def test_list_inside_struct_is_rejected(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(
        source,
        [
            pa.field(
                "fare",
                pa.struct([pa.field("tags", pa.list_(pa.string()))]),
                metadata={b"PARQUET:field_id": b"10"},
            )
        ],
        [{"fare": {"tags": ["x"]}}],
    )
    mapping = parse_name_mapping(
        json.dumps(
            [
                {
                    "field-id": 10,
                    "names": ["fare"],
                    "fields": [{"field-id": 11, "names": ["tags"]}],
                }
            ]
        )
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="list / map") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.columns == ["fare.tags"]


def test_missing_replica_is_reported_and_nothing_is_written(tmp_path):
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a"]}])
    )
    out = tmp_path / "out"
    with pytest.raises(CheckError, match="上传副本缺失") as exc_info:
        run_backfill(manifest, {}, mapping, out)
    assert exc_info.value.files == ["a.parquet"]
    assert not out.exists() or not any(out.iterdir())


def test_unreadable_parquet_is_reported(tmp_path):
    source = tmp_path / "a.parquet"
    source.write_bytes(b"not a parquet file")
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )
    with pytest.raises(CheckError, match="无法读取 Parquet") as exc_info:
        run_backfill(
            manifest,
            {"a.parquet": source},
            mapping,
            tmp_path / "out",
        )
    assert exc_info.value.files == ["a.parquet"]


def test_equality_delete_entries_are_ignored(tmp_path):
    source = tmp_path / "a.parquet"
    write_parquet(source, [pa.field("a", pa.int64())], [{"a": 1}])
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a"]}])
    )
    manifest = make_manifest(
        tmp_path,
        [
            ("s3://b/a.parquet", "a.parquet", "data", 1),
            ("s3://b/eq.parquet", "eq.parquet", "equality_delete", 2),
        ],
    )
    out = tmp_path / "out"
    result = run_backfill(manifest, {"a.parquet": source}, mapping, out)
    assert [fr.entry.uploaded_filename for fr in result.file_results] == [
        "a.parquet"
    ]
    assert any("equality_delete" in w for w in result.warnings)
    assert not (out / "eq.parquet").exists()


def test_all_or_nothing_no_output_when_second_file_fails(tmp_path):
    good = tmp_path / "good.parquet"
    bad = tmp_path / "bad.parquet"
    write_parquet(good, [pa.field("a", pa.int64())], [{"a": 1}])
    write_parquet(bad, [pa.field("unmapped", pa.int64())], [{"unmapped": 1}])
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a"]}])
    )
    manifest = make_manifest(
        tmp_path,
        [
            ("s3://b/good.parquet", "good.parquet", "data", 1),
            ("s3://b/bad.parquet", "bad.parquet", "data", 1),
        ],
    )
    out = tmp_path / "out"
    with pytest.raises(CheckError):
        run_backfill(
            manifest,
            {"good.parquet": good, "bad.parquet": bad},
            mapping,
            out,
        )
    # Phase 1 must finish before any rewrite: the output dir stays empty.
    assert not any(out.iterdir())


def test_write_failure_is_reported_and_partial_output_removed(tmp_path, monkeypatch):
    source = tmp_path / "a.parquet"
    write_parquet(source, [pa.field("a", pa.int64())], [{"a": 1}])
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a"]}])
    )
    manifest = make_manifest(
        tmp_path, [("s3://b/a.parquet", "a.parquet", "data", 1)]
    )

    def boom(*args, **kwargs):
        raise OSError("simulated disk failure")

    monkeypatch.setattr("checker.engine.pq.ParquetWriter", boom)
    out = tmp_path / "out"
    with pytest.raises(CheckError, match="重写 Parquet 文件") as exc_info:
        run_backfill(manifest, {"a.parquet": source}, mapping, out)
    assert exc_info.value.files == ["a.parquet"]
    assert not (out / "a.parquet").exists()
