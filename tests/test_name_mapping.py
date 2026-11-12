from __future__ import annotations

import json
from pathlib import Path

import pytest

from checker.errors import CheckError
from checker.name_mapping import MappingField, parse_name_mapping

MAPPING = [
    {
        "field-id": 3,
        "names": ["base_fare", "legacy_base_fare"],
        "fields": [
            {"field-id": 31, "names": ["fare_cents", "amount_cents"]},
            {"field-id": 32, "names": ["currency"]},
        ],
    },
    {
        "field-id": 4,
        "names": ["surcharge"],
        "fields": [
            {"field-id": 41, "names": ["fare_cents"]},
        ],
    },
    {"field-id": 1, "names": ["ticket_no"]},
]


def test_parse_nested_mapping_and_lookup_by_id():
    mapping = parse_name_mapping(json.dumps(MAPPING))
    assert len(mapping.roots) == 3
    assert mapping.by_id[3].names == ("base_fare", "legacy_base_fare")
    assert mapping.by_id[31].path == "base_fare.fare_cents"
    assert mapping.by_id[41].path == "surcharge.fare_cents"


def test_top_level_matching_uses_exact_alias():
    mapping = parse_name_mapping(json.dumps(MAPPING))
    assert [m.field_id for m in mapping.top_level_matches("legacy_base_fare")] == [3]
    assert mapping.top_level_matches("base") == []


def test_child_lookup_goes_through_parent_id():
    mapping = parse_name_mapping(json.dumps(MAPPING))
    parent, children = mapping.child_matches(3, "fare_cents")
    assert parent is not None and parent.field_id == 3
    assert [c.field_id for c in children] == [31]

    parent, children = mapping.child_matches(4, "fare_cents")
    assert parent is not None and parent.field_id == 4
    assert [c.field_id for c in children] == [41]

    parent, children = mapping.child_matches(999, "fare_cents")
    assert parent is None
    assert children == []


def test_underscore_and_object_container_are_accepted():
    mapping = parse_name_mapping(
        json.dumps({"fields": [{"field_id": 7, "name": "ticket"}]})
    )
    assert mapping.roots[0].field_id == 7
    assert mapping.roots[0].names == ("ticket",)


def test_missing_names_is_rejected():
    with pytest.raises(CheckError, match="names"):
        parse_name_mapping(json.dumps([{"field-id": 1}]))


def test_non_integer_id_is_rejected():
    with pytest.raises(CheckError, match="field-id"):
        parse_name_mapping(json.dumps([{"field-id": "abc", "names": ["a"]}]))


def test_non_positive_id_is_rejected():
    with pytest.raises(CheckError, match="正整数"):
        parse_name_mapping(json.dumps([{"field-id": 0, "names": ["a"]}]))


def test_duplicate_id_is_rejected():
    with pytest.raises(CheckError, match="重复"):
        parse_name_mapping(
            json.dumps(
                [
                    {"field-id": 1, "names": ["a"]},
                    {"field-id": 2, "names": ["b"], "fields": [
                        {"field-id": 1, "names": ["c"]},
                    ]},
                ]
            )
        )


def test_invalid_json_is_rejected():
    with pytest.raises(CheckError, match="不是有效的 JSON"):
        parse_name_mapping("{not json")


def test_empty_mapping_is_rejected():
    with pytest.raises(CheckError, match="为空"):
        parse_name_mapping("  ")
    with pytest.raises(CheckError, match="没有字段条目"):
        parse_name_mapping("[]")


def test_empty_names_list_is_rejected():
    with pytest.raises(CheckError, match="没有可用名称"):
        parse_name_mapping(json.dumps([{"field-id": 1, "names": []}]))
