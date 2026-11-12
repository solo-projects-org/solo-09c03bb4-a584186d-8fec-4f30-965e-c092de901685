"""Generate the bundled sample for the field-ID backfiller.

The legacy replicas are written *without* most Parquet field IDs, the way an
old writer would have produced them before the table got an Iceberg schema.
The bundled ``name-mapping.json`` is the JSON content of the table property
``schema.name-mapping.default``.

Demonstration points:

* Two structs ``deck_fare`` and ``cabin_fare`` both contain a child column
  called ``amount``.  Their IDs differ (21 vs 31); the engine must reach the
  right sub-mapping through each struct's own field ID instead of matching
  the leaf name globally.
* ``fare_note`` is missing from the Parquet column names but the mapping
  aliases it as ``remark``, exercising alias-only matching.
* ``ticket_no`` already carries field ID 1 and must be left untouched.
* The positional delete files also need backfilling (file_path=41, pos=42);
  the manifest and the delete files keep working with the rewritten data
  replicas because names, types, values and row order do not change.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample"

BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1_PATH = f"{BASE}/data/data-0001.parquet"
DATA2_PATH = f"{BASE}/data/data-0002.parquet"
DELETE1_PATH = f"{BASE}/delete-pos/delete-pos-0001.parquet"
DELETE2_PATH = f"{BASE}/delete-pos/delete-pos-0002.parquet"
EQ_DELETE_PATH = f"{BASE}/delete-eq/delete-eq-0001.parquet"

# Columns deliberately start without Iceberg field IDs, except ticket_no
# which already has one and must be preserved as-is.
DATA_SCHEMA = pa.schema(
    [
        pa.field("ticket_no", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
        pa.field("passenger_name", pa.string()),
        pa.field(
            "deck_fare",
            pa.struct(
                [
                    pa.field("amount", pa.int64()),
                    pa.field("currency", pa.string()),
                ]
            ),
        ),
        pa.field(
            "cabin_fare",
            pa.struct(
                [
                    pa.field("amount", pa.int64()),
                    pa.field("currency", pa.string()),
                ]
            ),
        ),
        pa.field("fare_note", pa.string()),
        pa.field("voyage_date", pa.string()),
    ]
)

DELETE_SCHEMA = pa.schema(
    [
        pa.field("file_path", pa.string()),
        pa.field("pos", pa.int64()),
    ]
)

NAME_MAPPING = [
    {
        "field-id": 1,
        "names": ["ticket_no"],
    },
    {
        "field-id": 2,
        "names": ["passenger_name", "passenger"],
    },
    {
        "field-id": 20,
        "names": ["deck_fare", "deck_settlement"],
        "fields": [
            {"field-id": 21, "names": ["amount", "fare_amount", "price"]},
            {"field-id": 22, "names": ["currency", "ccy"]},
        ],
    },
    {
        "field-id": 30,
        "names": ["cabin_fare", "cabin_settlement"],
        "fields": [
            {"field-id": 31, "names": ["amount", "fare_amount", "price"]},
            {"field-id": 32, "names": ["currency", "ccy"]},
        ],
    },
    {
        "field-id": 5,
        "names": ["fare_note", "remark"],
    },
    {
        "field-id": 6,
        "names": ["voyage_date"],
    },
    {
        "field-id": 41,
        "names": ["file_path", "path"],
    },
    {
        "field-id": 42,
        "names": ["pos", "position"],
    },
]


def data_rows(tickets: list[int]) -> list[dict]:
    return [
        {
            "ticket_no": ticket,
            "passenger_name": f"P-{ticket}",
            "deck_fare": {
                "amount": 1500 + (ticket % 700),
                "currency": "CNY" if ticket % 2 == 0 else "USD",
            },
            "cabin_fare": {
                "amount": 3200 + (ticket % 300),
                "currency": "CNY",
            },
            "fare_note": "online" if ticket % 3 else "window",
            "voyage_date": "2026-11-05",
        }
        for ticket in tickets
    ]


def write_data1() -> None:
    tickets = list(range(1000, 1024)) + list(range(2000, 2012))
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # Two row groups so the rewrite must preserve row-group boundaries.
    pq.write_table(
        table,
        SAMPLE / "data-0001.parquet",
        row_group_size=18,
        compression="snappy",
    )


def write_data2() -> None:
    tickets = list(range(7000, 7009)) + [8888]
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # Mixed compression plus disabled statistics: both must survive rewriting.
    pq.write_table(
        table,
        SAMPLE / "data-0002.parquet",
        row_group_size=5,
        compression={
            "ticket_no": "gzip",
            "deck_fare.amount": "zstd",
            "deck_fare.currency": "snappy",
            "cabin_fare.amount": "zstd",
            "cabin_fare.currency": "snappy",
        },
        write_statistics=False,
    )


def write_delete(path: Path, records: list[tuple[str, int]]) -> None:
    table = pa.Table.from_pylist(
        [{"file_path": file_path, "pos": pos} for file_path, pos in records],
        schema=DELETE_SCHEMA,
    )
    pq.write_table(table, path, row_group_size=10, compression="snappy")


def write_name_mapping() -> None:
    (SAMPLE / "name-mapping.json").write_text(
        json.dumps(NAME_MAPPING, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_manifest() -> None:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-0001.parquet,data,7",
        f"{DATA2_PATH},data-0002.parquet,data,7",
        f"{DELETE1_PATH},delete-pos-0001.parquet,position_delete,12",
        f"{DELETE2_PATH},delete-pos-0002.parquet,position_delete,8",
        f"{EQ_DELETE_PATH},delete-eq-0001.parquet,equality_delete,12",
    ]
    (SAMPLE / "manifest.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    SAMPLE.mkdir(parents=True, exist_ok=True)
    write_data1()
    write_data2()
    write_delete(
        SAMPLE / "delete-pos-0001.parquet",
        [
            (DATA1_PATH, 3),
            (DATA1_PATH, 19),
        ],
    )
    write_delete(
        SAMPLE / "delete-pos-0002.parquet",
        [(DATA2_PATH, 9)],
    )
    write_manifest()
    write_name_mapping()

    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "manifest.csv",
        "name-mapping.json",
    ):
        print(f"wrote {SAMPLE / name}")


if __name__ == "__main__":
    main()
