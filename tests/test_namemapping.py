from __future__ import annotations

import json

import pytest

from checker.errors import CheckError
from checker.namemapping import NameMapping, parse_name_mapping


def test_parse_array_mapping_with_nested_children():
    mapping = parse_name_mapping(
        json.dumps(
            [
                {"field-id": 1, "names": ["ticket_no"]},
                {
                    "field-id": 20,
                    "names": ["deck_fare", "deck_settlement"],
                    "fields": [
                        {"field-id": 21, "names": ["amount", "fare_amount"]},
                        {"field-id": 22, "names": ["currency"]},
                    ],
                },
            ]
        )
    )
    assert isinstance(mapping, NameMapping)
    assert [node.field_id for node in mapping.roots] == [1, 20]
    deck = mapping.node_by_id(20)
    assert deck is not None
    assert [child.field_id for child in deck.children] == [21, 22]
    assert mapping.top_level_by_name("ticket_no")[0].field_id == 1
    # Alias lookup.
    assert mapping.top_level_by_name("deck_settlement")[0].field_id == 20
    # Scoped child lookup.
    assert deck.child_by_name("fare_amount")[0].field_id == 21


def test_single_object_and_wrapped_forms_are_accepted():
    single = parse_name_mapping(json.dumps({"field-id": 1, "names": ["a"]}))
    assert single.roots[0].field_id == 1

    wrapped = parse_name_mapping(
        json.dumps({"fields": [{"field-id": 2, "names": ["b"]}]})
    )
    assert wrapped.roots[0].field_id == 2


def test_empty_input_is_rejected():
    with pytest.raises(CheckError, match="name-mapping 为空"):
        parse_name_mapping("   ")


def test_invalid_json_is_rejected():
    with pytest.raises(CheckError, match="JSON 解析失败"):
        parse_name_mapping("[{")


def test_missing_field_id_is_rejected():
    with pytest.raises(CheckError, match="field-id"):
        parse_name_mapping(json.dumps([{"names": ["a"]}]))


def test_negative_field_id_is_rejected():
    with pytest.raises(CheckError, match="不能为负数"):
        parse_name_mapping(json.dumps([{"field-id": -1, "names": ["a"]}]))


def test_boolean_field_id_is_rejected():
    with pytest.raises(CheckError, match="整数"):
        parse_name_mapping(json.dumps([{"field-id": True, "names": ["a"]}]))


def test_missing_names_is_rejected():
    with pytest.raises(CheckError, match="names"):
        parse_name_mapping(json.dumps([{"field-id": 1}]))


def test_duplicate_field_ids_are_rejected():
    payload = [
        {"field-id": 1, "names": ["a"]},
        {"field-id": 1, "names": ["b"]},
    ]
    with pytest.raises(CheckError, match="field-id=1 重复"):
        parse_name_mapping(json.dumps(payload))


def test_sibling_name_collision_is_kept_for_engine_to_reject():
    # The parser accepts ambiguous mappings; uniqueness is enforced per file
    # by the engine, which can name the offending file and column.
    payload = [
        {"field-id": 1, "names": ["shared"]},
        {"field-id": 2, "names": ["shared", "other"]},
    ]
    mapping = parse_name_mapping(json.dumps(payload))
    assert {node.field_id for node in mapping.top_level_by_name("shared")} == {
        1,
        2,
    }


def test_same_name_under_different_parents_is_allowed():
    # The two "amount" names live in different scopes and must be accepted.
    payload = [
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
    mapping = parse_name_mapping(json.dumps(payload))
    assert mapping.node_by_id(21) is not None
    assert mapping.node_by_id(31) is not None


def test_node_by_id_returns_none_for_unknown_id():
    mapping = parse_name_mapping(json.dumps([{"field-id": 1, "names": ["a"]}]))
    assert mapping.node_by_id(999) is None


def test_snake_case_and_alternative_key_names_are_accepted():
    mapping = parse_name_mapping(
        json.dumps(
            [
                {
                    "field_id": 20,
                    "names": ["fare"],
                    "fields": [{"field_id": 21, "names": ["amount"]}],
                }
            ]
        )
    )
    parent = mapping.node_by_id(20)
    assert parent is not None
    assert parent.child_by_name("amount")[0].field_id == 21


def test_zero_field_id_is_valid():
    mapping = parse_name_mapping(json.dumps([{"field-id": 0, "names": ["z"]}]))
    assert mapping.roots[0].field_id == 0


def test_duplicate_alias_names_are_deduplicated():
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 1, "names": ["a", "a", "b"]}])
    )
    assert mapping.roots[0].names == ("a", "b")


def test_empty_children_list_is_accepted_by_parser():
    mapping = parse_name_mapping(
        json.dumps([{"field-id": 10, "names": ["fare"], "fields": []}])
    )
    assert mapping.node_by_id(10).children == ()
