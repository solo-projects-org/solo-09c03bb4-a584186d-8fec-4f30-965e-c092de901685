"""Parsing of the Iceberg ``schema.name-mapping.default`` table property.

The property is a JSON array of field mappings.  Each mapping entry has a
``field-id``, a list of ``names`` (aliases), and, for struct fields, a nested
``fields`` list.  This module only needs one level of nesting for the field-ID
backfill, but the parser validates the complete JSON so that malformed input
is reported before any Parquet file is rewritten.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .errors import CheckError

_FIELD_ID_KEYS = {"field-id", "field-id".replace("-", ""), "id"}
_NAMES_KEYS = {"names", "name", "aliases"}
_FIELDS_KEYS = {"fields", "children"}
_CONTAINER_KEYS = {"fields", "mapping", "name-mapping", "name_mapping"}


def _normalise_key(key: object) -> str:
    return str(key).strip().lower().replace("_", "-")


def _pick(entry: dict[object, object], wanted: set[str]) -> object | None:
    for raw_key, value in entry.items():
        if _normalise_key(raw_key) in wanted:
            return value
    return None


@dataclass(frozen=True)
class MappingField:
    field_id: int
    names: tuple[str, ...]
    fields: tuple["MappingField", ...] = ()
    path: str = ""

    @property
    def display_name(self) -> str:
        return self.names[0] if self.names else self.path


@dataclass
class NameMapping:
    roots: tuple[MappingField, ...]
    by_id: dict[int, MappingField] = field(default_factory=dict)

    def top_level_matches(self, name: str) -> list[MappingField]:
        """Mapping entries whose aliases contain ``name`` exactly."""
        return [entry for entry in self.roots if name in entry.names]

    def child_matches(
        self, parent_id: int, name: str
    ) -> tuple[MappingField | None, list[MappingField]]:
        """Return the parent mapping entry for ``parent_id`` and child
        entries whose aliases contain ``name`` exactly.

        Child matching deliberately goes through the *parent field ID* rather
        than the parent name.  Two structs may both contain a column called
        ``amount``; only the parent ID says which mapping branch applies.
        """
        parent = self.by_id.get(parent_id)
        if parent is None:
            return None, []
        return parent, [c for c in parent.fields if name in c.names]


def _parse_field_id(raw: object, *, path: str) -> int:
    if isinstance(raw, bool):
        raise CheckError(f"name mapping 中字段 “{path}” 的 field-id 不是整数。")
    try:
        value = int(str(raw).strip(), 10)
    except (TypeError, ValueError):
        raise CheckError(
            f"name mapping 中字段 “{path}” 的 field-id “{raw}” 不是整数。"
        ) from None
    if value <= 0:
        raise CheckError(
            f"name mapping 中字段 “{path}” 的 field-id {value} 必须为正整数。"
        )
    return value


def _parse_names(raw: object, *, path: str) -> tuple[str, ...]:
    if isinstance(raw, str):
        candidates = [raw]
    elif isinstance(raw, list):
        candidates = raw
    else:
        raise CheckError(
            f"name mapping 中字段 “{path}” 的 names 必须是字符串数组。"
        )
    names: list[str] = []
    for item in candidates:
        if not isinstance(item, str):
            raise CheckError(
                f"name mapping 中字段 “{path}” 的 names 含有非字符串值。"
            )
        name = item.strip()
        if not name:
            raise CheckError(
                f"name mapping 中字段 “{path}” 的 names 含有空名称。"
            )
        if name not in names:
            names.append(name)
    if not names:
        raise CheckError(f"name mapping 中字段 “{path}” 没有可用名称。")
    return tuple(names)


def _parse_nodes(
    items: object, *, parent_path: str, seen_ids: dict[int, str]
) -> tuple[MappingField, ...]:
    if not isinstance(items, list):
        raise CheckError("name mapping 的字段条目必须是数组。")
    nodes: list[MappingField] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise CheckError(
                f"name mapping 第 {index} 个字段条目不是对象。"
            )
        raw_id = _pick(item, _FIELD_ID_KEYS)
        if raw_id is None:
            raise CheckError(
                f"name mapping 第 {index} 个字段条目缺少 field-id。"
            )
        # The name is needed to build a useful path for error messages.
        raw_names = _pick(item, _NAMES_KEYS)
        provisional_path = f"{parent_path}.第{index}项" if parent_path else f"第{index}项"
        names = _parse_names(raw_names, path=provisional_path)
        node_path = (
            f"{parent_path}.{names[0]}" if parent_path else names[0]
        )
        field_id = _parse_field_id(raw_id, path=node_path)
        previous = seen_ids.get(field_id)
        if previous is not None:
            raise CheckError(
                f"name mapping 中字段 ID {field_id} 重复："
                f"“{previous}” 与 “{node_path}”。"
            )
        seen_ids[field_id] = node_path

        raw_children = _pick(item, _FIELDS_KEYS)
        children: tuple[MappingField, ...] = ()
        if raw_children is not None:
            children = _parse_nodes(
                raw_children,
                parent_path=node_path,
                seen_ids=seen_ids,
            )
        nodes.append(
            MappingField(
                field_id=field_id,
                names=names,
                fields=children,
                path=node_path,
            )
        )
    return tuple(nodes)


def parse_name_mapping(data: bytes | str) -> NameMapping:
    """Parse the JSON value of ``schema.name-mapping.default``."""
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CheckError(
                f"name mapping 不是有效的 UTF-8 文本：{exc}"
            ) from None
    else:
        text = data
    text = text.lstrip("﻿ \t\r\n")
    if not text:
        raise CheckError("name mapping 内容为空。")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CheckError(f"name mapping 不是有效的 JSON：{exc}") from None

    if isinstance(payload, dict):
        container = _pick(payload, _CONTAINER_KEYS)
        if container is None:
            raise CheckError(
                "name mapping 应为字段数组，或包含 fields / mapping 数组的对象。"
            )
        payload = container
    if not isinstance(payload, list):
        raise CheckError("name mapping 顶层必须是数组。")
    if not payload:
        raise CheckError("name mapping 中没有字段条目。")

    seen_ids: dict[int, str] = {}
    roots = _parse_nodes(payload, parent_path="", seen_ids=seen_ids)
    by_id = {node.field_id: node for node in _walk(roots)}
    return NameMapping(roots=roots, by_id=by_id)


def _walk(nodes: tuple[MappingField, ...]) -> list[MappingField]:
    result: list[MappingField] = []
    for node in nodes:
        result.append(node)
        result.extend(_walk(node.fields))
    return result
