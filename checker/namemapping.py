"""Parsing of the Iceberg table property ``schema.name-mapping.default``.

The property value is a JSON document using the Iceberg name-mapping format::

    {
      "field-id": 20,
      "names": ["deck_fare"],
      "fields": [
        {"field-id": 21, "names": ["amount", "fare_amount"]},
        {"field-id": 22, "names": ["currency"]}
      ]
    }

A parsed mapping is a tree of :class:`MappingNode`.  Lookups are always
scoped:

* top-level Parquet fields match against the root nodes;
* columns of a struct match against the children of *the node whose
  field-id equals the struct's existing field id* ("子列先用父字段的 ID
  找到对应的子映射").

Every lookup returns the list of matching nodes so that the engine can
reject ambiguous matches instead of guessing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .errors import CheckError

_ID_KEYS = ("field-id", "field_id", "id", "fieldId", "fieldid")
_NAMES_KEYS = ("names", "name")
_CHILD_KEYS = ("fields", "mappings", "children")


@dataclass(frozen=True)
class MappingNode:
    field_id: int
    names: tuple[str, ...]
    children: tuple["MappingNode", ...] = field(default_factory=tuple)

    def child_by_name(self, name: str) -> list["MappingNode"]:
        return [node for node in self.children if name in node.names]


@dataclass
class NameMapping:
    roots: tuple[MappingNode, ...]

    def top_level_by_name(self, name: str) -> list[MappingNode]:
        return [node for node in self.roots if name in node.names]

    def node_by_id(self, field_id: int) -> MappingNode | None:
        stack: list[MappingNode] = list(self.roots)
        while stack:
            node = stack.pop()
            if node.field_id == field_id:
                return node
            stack.extend(node.children)
        return None


def _first_key(payload: dict, keys: tuple[str, ...]) -> object:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _parse_node(payload: object, path_label: str) -> MappingNode:
    if not isinstance(payload, dict):
        raise CheckError(
            f"name-mapping 的 {path_label} 必须是对象，实际为 "
            f"{type(payload).__name__}。"
        )

    raw_id = _first_key(payload, _ID_KEYS)
    if raw_id is None or isinstance(raw_id, bool) or not isinstance(raw_id, int):
        raise CheckError(
            f"name-mapping 的 {path_label} 缺少整数类型的 field-id。"
        )
    if raw_id < 0:
        raise CheckError(
            f"name-mapping 的 {path_label} 的 field-id={raw_id} 不能为负数；"
            "Iceberg 字段 ID 从 0 起算。"
        )

    raw_names = _first_key(payload, _NAMES_KEYS)
    names: list[str] = []
    if isinstance(raw_names, str):
        names = [raw_names]
    elif isinstance(raw_names, list):
        for item in raw_names:
            if not isinstance(item, str) or not item.strip():
                raise CheckError(
                    f"name-mapping 的 field-id={raw_id} 节点的 names 必须是"
                    "非空字符串数组。"
                )
            names.append(item)
    elif raw_names is None:
        raise CheckError(
            f"name-mapping 的 field-id={raw_id} 节点缺少 names。"
        )
    else:
        raise CheckError(
            f"name-mapping 的 field-id={raw_id} 节点的 names 必须是字符串或"
            "字符串数组。"
        )
    names = tuple(dict.fromkeys(name.strip() for name in names))
    if any(not name for name in names):
        raise CheckError(
            f"name-mapping 的 field-id={raw_id} 节点存在空白列名。"
        )

    raw_children = _first_key(payload, _CHILD_KEYS)
    children: list[MappingNode] = []
    if raw_children is not None:
        if not isinstance(raw_children, list):
            raise CheckError(
                f"name-mapping 的 field-id={raw_id} 节点的 fields 必须是数组。"
            )
        for index, child in enumerate(raw_children):
            children.append(
                _parse_node(
                    child,
                    f"field-id={raw_id} 节点的第 {index + 1} 个子节点",
                )
            )

    return MappingNode(
        field_id=raw_id, names=names, children=tuple(children)
    )


def _check_duplicate_ids(
    nodes: tuple[MappingNode, ...] | list[MappingNode],
    seen: dict[int, str],
) -> None:
    for node in nodes:
        label = "/".join(node.names) or f"field-id={node.field_id}"
        previous = seen.get(node.field_id)
        if previous is not None:
            raise CheckError(
                f"name-mapping 中 field-id={node.field_id} 重复：同时出现在 "
                f"“{previous}” 和 “{label}”。每个字段 ID 必须唯一。",
                columns=[label],
            )
        seen[node.field_id] = label
        _check_duplicate_ids(node.children, seen)


def parse_name_mapping(data: bytes | str) -> NameMapping:
    """Parse the JSON text of ``schema.name-mapping.default``."""
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CheckError(f"name-mapping 不是有效的 UTF-8 文本：{exc}") from None
    else:
        text = data
    if not text.strip():
        raise CheckError("name-mapping 为空，请粘贴 schema.name-mapping.default 的 JSON 内容。")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CheckError(f"name-mapping JSON 解析失败：{exc}") from None

    # The property may hold a bare array, a single mapping object, or an
    # object wrapping the array in a "fields" key.
    if isinstance(payload, list):
        raw_nodes = payload
    elif isinstance(payload, dict):
        wrapped = _first_key(payload, _CHILD_KEYS)
        if isinstance(wrapped, list):
            raw_nodes = wrapped
        elif _first_key(payload, _ID_KEYS) is not None:
            raw_nodes = [payload]
        else:
            raise CheckError(
                "name-mapping 顶层对象必须包含 fields 数组，或本身是一个带 "
                "field-id 的映射节点。"
            )
    else:
        raise CheckError("name-mapping 顶层必须是数组或对象。")

    if not raw_nodes:
        raise CheckError("name-mapping 不包含任何字段映射。")

    nodes = tuple(
        _parse_node(item, f"第 {index + 1} 个顶层节点")
        for index, item in enumerate(raw_nodes)
    )
    _check_duplicate_ids(nodes, {})
    return NameMapping(roots=nodes)
