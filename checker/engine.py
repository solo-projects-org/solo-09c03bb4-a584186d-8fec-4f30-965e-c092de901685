"""Backfill Iceberg field IDs into legacy Parquet replicas.

The engine rewrites each Parquet replica listed in the valid-file manifest so
that the Iceberg field IDs from ``schema.name-mapping.default`` are written
into the Parquet schema metadata.

Rules implemented here:

* Only scalar columns and one level of ``struct`` are handled.  Lists, maps
  and nested structs are rejected with the file and column path named.
* Fields that already carry a valid Parquet field ID are left untouched;
  fields without an ID are matched against the name mapping **by name and
  aliases** (the mapping's ``names`` array).  A match must be unique.
* Struct children are scoped: they are matched inside the mapping node whose
  ``field-id`` equals the struct's own field ID.
* Field IDs must be unique within a single output file.
* Column names, Arrow types, nullability, values and row order are preserved;
  files are rewritten one row group at a time, keeping row-group boundaries,
  per-column compression, schema-level key/value metadata and statistics
  settings.
* The whole batch is all-or-nothing: every replica is validated before any
  file is rewritten.  Missing files, ambiguous matches, duplicate IDs or any
  read/write failure raise a :class:`CheckError` naming the file(s) and
  column(s) involved, and no download is offered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import CheckError
from .manifest import Manifest, ManifestEntry
from .namemapping import MappingNode, NameMapping

FIELD_ID_KEY = b"PARQUET:field_id"
_UNCOMPRESSED = "UNCOMPRESSED"


@dataclass(frozen=True)
class FieldChange:
    """One column whose field ID was backfilled in one file."""

    column_path: str
    field_id: int
    matched_names: tuple[str, ...]


@dataclass
class FilePlan:
    entry: ManifestEntry
    source_path: Path
    output_schema: pa.Schema
    changes: list[FieldChange]
    kept_ids: list[tuple[str, int]]
    compression: dict[str, str]
    write_statistics: bool
    num_row_groups: int


@dataclass
class FileResult:
    entry: ManifestEntry
    output_path: Path
    changes: list[FieldChange]
    kept_ids: list[tuple[str, int]]
    num_row_groups: int


@dataclass
class BackfillResult:
    manifest: Manifest
    file_results: list[FileResult]
    unused_uploads: list[str]
    warnings: list[str] = field(default_factory=list)

    @property
    def total_assigned(self) -> int:
        return sum(len(fr.changes) for fr in self.file_results)

    @property
    def total_kept(self) -> int:
        return sum(len(fr.kept_ids) for fr in self.file_results)


# --------------------------------------------------------------------------- #
# Field-ID helpers
# --------------------------------------------------------------------------- #


def existing_field_id(arrow_field: pa.Field) -> int | None:
    """Return the valid Parquet field ID already on a field, else ``None``.

    Missing metadata, a non-numeric value or the Parquet sentinel ``-1`` all
    mean "no Iceberg ID yet": such fields are backfilled rather than trusted.
    """
    metadata = arrow_field.metadata
    if not metadata or FIELD_ID_KEY not in metadata:
        return None
    raw = metadata[FIELD_ID_KEY]
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def _with_field_id(arrow_field: pa.Field, field_id: int) -> pa.Field:
    metadata = dict(arrow_field.metadata or {})
    metadata[FIELD_ID_KEY] = str(field_id).encode("ascii")
    return pa.field(
        arrow_field.name,
        arrow_field.type,
        nullable=arrow_field.nullable,
        metadata=metadata,
    )


# --------------------------------------------------------------------------- #
# Phase 1: pure schema planning (no files are written)
# --------------------------------------------------------------------------- #


def _unique_match(
    candidates: list[MappingNode],
    *,
    column_path: str,
    file_label: str,
    scope_description: str,
) -> MappingNode:
    if not candidates:
        raise CheckError(
            f"文件 “{file_label}” 的列 “{column_path}” 缺少字段 ID，且在"
            f"{scope_description}中没有任何映射名称与其匹配。请检查 "
            "schema.name-mapping.default 是否包含该列或其别名。",
            files=[file_label],
            columns=[column_path],
        )
    distinct = {node.field_id: node for node in candidates}
    if len(distinct) > 1:
        raise CheckError(
            f"文件 “{file_label}” 的列 “{column_path}” 缺少字段 ID，且在"
            f"{scope_description}中同时匹配到多个字段 ID："
            + "、".join(
                f"{field_id}（名称 {'/'.join(node.names)}）"
                for field_id, node in sorted(distinct.items())
            )
            + "。匹配必须唯一。",
            files=[file_label],
            columns=[column_path],
        )
    return next(iter(distinct.values()))


def _claim_id(
    field_id: int, *, column_path: str, file_label: str, used_ids: dict[int, str]
) -> None:
    previous = used_ids.get(field_id)
    if previous is not None and previous != column_path:
        raise CheckError(
            f"文件 “{file_label}” 内字段 ID {field_id} 同时出现在列 "
            f"“{previous}” 和 “{column_path}”，单文件内字段 ID 必须唯一。",
            files=[file_label],
            columns=[previous, column_path],
        )
    used_ids[field_id] = column_path


def _resolve_id(
    arrow_field: pa.Field,
    *,
    column_path: str,
    file_label: str,
    scope_nodes: tuple[MappingNode, ...] | None,
    scope_description: str,
    used_ids: dict[int, str],
    changes: list[FieldChange],
    kept: list[tuple[str, int]],
) -> tuple[int, pa.Field]:
    """Return the field ID and a (possibly rewritten) field carrying it."""
    field_id = existing_field_id(arrow_field)
    if field_id is not None:
        _claim_id(
            field_id,
            column_path=column_path,
            file_label=file_label,
            used_ids=used_ids,
        )
        kept.append((column_path, field_id))
        return field_id, arrow_field

    if not scope_nodes:
        raise CheckError(
            f"文件 “{file_label}” 的列 “{column_path}” 缺少字段 ID，且在"
            f"{scope_description}中没有可用于匹配的子映射。",
            files=[file_label],
            columns=[column_path],
        )
    node = _unique_match(
        [candidate for candidate in scope_nodes if arrow_field.name in candidate.names],
        column_path=column_path,
        file_label=file_label,
        scope_description=scope_description,
    )
    field_id = node.field_id
    _claim_id(
        field_id,
        column_path=column_path,
        file_label=file_label,
        used_ids=used_ids,
    )
    changes.append(
        FieldChange(
            column_path=column_path,
            field_id=field_id,
            matched_names=node.names,
        )
    )
    return field_id, _with_field_id(arrow_field, field_id)


def _plan_scalar(
    arrow_field: pa.Field,
    *,
    column_path: str,
    file_label: str,
    scope_nodes: tuple[MappingNode, ...] | None,
    scope_description: str,
    used_ids: dict[int, str],
    changes: list[FieldChange],
    kept: list[tuple[str, int]],
) -> pa.Field:
    if (
        pa.types.is_list(arrow_field.type)
        or pa.types.is_large_list(arrow_field.type)
        or pa.types.is_map(arrow_field.type)
    ):
        raise CheckError(
            f"文件 “{file_label}” 的列 “{column_path}” 类型为 "
            f"{arrow_field.type}；本工具只处理标量列和一层 struct，"
            "不支持 list / map。",
            files=[file_label],
            columns=[column_path],
        )
    _field_id, rewritten = _resolve_id(
        arrow_field,
        column_path=column_path,
        file_label=file_label,
        scope_nodes=scope_nodes,
        scope_description=scope_description,
        used_ids=used_ids,
        changes=changes,
        kept=kept,
    )
    return rewritten


def _plan_struct(
    arrow_field: pa.Field,
    *,
    column_path: str,
    file_label: str,
    scope_nodes: tuple[MappingNode, ...] | None,
    scope_description: str,
    used_ids: dict[int, str],
    changes: list[FieldChange],
    kept: list[tuple[str, int]],
    name_mapping: NameMapping,
) -> pa.Field:
    struct_id, rewritten_struct = _resolve_id(
        arrow_field,
        column_path=column_path,
        file_label=file_label,
        scope_nodes=scope_nodes,
        scope_description=scope_description,
        used_ids=used_ids,
        changes=changes,
        kept=kept,
    )

    # Sub-columns are resolved strictly through the mapping node selected by
    # the parent struct's own field ID.
    parent_node = name_mapping.node_by_id(struct_id)
    child_scope = parent_node.children if parent_node is not None else None
    child_scope_description = (
        f"父字段 “{column_path}”（field-id {struct_id}）的子映射"
    )

    new_children: list[pa.Field] = []
    for child in arrow_field.type:
        child_path = f"{column_path}.{child.name}"
        if pa.types.is_struct(child.type):
            raise CheckError(
                f"文件 “{file_label}” 的列 “{child_path}” 是嵌套 struct；"
                "本工具只处理一层 struct。",
                files=[file_label],
                columns=[child_path],
            )
        new_children.append(
            _plan_scalar(
                child,
                column_path=child_path,
                file_label=file_label,
                scope_nodes=child_scope,
                scope_description=child_scope_description,
                used_ids=used_ids,
                changes=changes,
                kept=kept,
            )
        )

    new_type = pa.struct(
        [
            pa.field(
                child.name,
                child.type,
                nullable=child.nullable,
                metadata=child.metadata,
            )
            for child in new_children
        ]
    )
    return pa.field(
        arrow_field.name,
        new_type,
        nullable=arrow_field.nullable,
        metadata=rewritten_struct.metadata,
    )


def _compression_map(parquet_file: pq.ParquetFile) -> tuple[dict[str, str], bool]:
    """Read per-leaf-path compression and whether statistics exist."""
    metadata = parquet_file.metadata
    first_group = metadata.row_group(0)
    compression: dict[str, str] = {}
    stats_set = False
    for row_group_index in range(metadata.num_row_groups):
        group = metadata.row_group(row_group_index)
        for column_index in range(metadata.num_columns):
            column = group.column(column_index)
            codec = str(column.compression)
            if codec != _UNCOMPRESSED:
                compression[column.path_in_schema] = codec.lower()
            if column.is_stats_set:
                stats_set = True
    # Keep one reference of every leaf path even when uncompressed.
    for column_index in range(metadata.num_columns):
        path = first_group.column(column_index).path_in_schema
        compression.setdefault(path, "none")
    return compression, stats_set


def _plan_file(
    entry: ManifestEntry,
    source_path: Path,
    name_mapping: NameMapping,
) -> FilePlan:
    label = entry.uploaded_filename
    try:
        parquet_file = pq.ParquetFile(str(source_path))
        source_schema = parquet_file.schema_arrow
    except Exception as exc:
        raise CheckError(
            f"无法读取 Parquet 文件 “{label}”（原始路径 {entry.original_path}）："
            f"{exc}",
            files=[label],
        ) from None

    changes: list[FieldChange] = []
    kept: list[tuple[str, int]] = []
    used_ids: dict[int, str] = {}
    new_fields: list[pa.Field] = []

    for arrow_field in source_schema:
        if pa.types.is_struct(arrow_field.type):
            new_fields.append(
                _plan_struct(
                    arrow_field,
                    column_path=arrow_field.name,
                    file_label=label,
                    scope_nodes=name_mapping.roots,
                    scope_description="name-mapping 顶层映射",
                    used_ids=used_ids,
                    changes=changes,
                    kept=kept,
                    name_mapping=name_mapping,
                )
            )
        else:
            new_fields.append(
                _plan_scalar(
                    arrow_field,
                    column_path=arrow_field.name,
                    file_label=label,
                    scope_nodes=name_mapping.roots,
                    scope_description="name-mapping 顶层映射",
                    used_ids=used_ids,
                    changes=changes,
                    kept=kept,
                )
            )

    output_schema = pa.schema(new_fields, metadata=source_schema.metadata)
    compression, stats_set = _compression_map(parquet_file)

    return FilePlan(
        entry=entry,
        source_path=source_path,
        output_schema=output_schema,
        changes=changes,
        kept_ids=kept,
        compression=compression,
        write_statistics=stats_set,
        num_row_groups=parquet_file.num_row_groups,
    )


# --------------------------------------------------------------------------- #
# Phase 2: row-group-preserving rewrite
# --------------------------------------------------------------------------- #


def _rewrite_file(plan: FilePlan, output_dir: Path) -> FileResult:
    label = plan.entry.uploaded_filename
    output_path = output_dir / label
    try:
        source = pq.ParquetFile(str(plan.source_path))
        with pq.ParquetWriter(
            str(output_path),
            plan.output_schema,
            compression=plan.compression,
            write_statistics=plan.write_statistics,
        ) as writer:
            for row_group_index in range(source.num_row_groups):
                chunk = source.read_row_group(row_group_index)
                writer.write_table(chunk.cast(plan.output_schema))
    except CheckError:
        raise
    except Exception as exc:
        if output_path.exists():
            output_path.unlink(missing_ok=True)
        raise CheckError(
            f"重写 Parquet 文件 “{label}” 失败：{exc}",
            files=[label],
            columns=[change.column_path for change in plan.changes],
        ) from None

    try:
        rewritten = pq.ParquetFile(str(output_path))
        if rewritten.num_row_groups != plan.num_row_groups:
            raise CheckError(
                f"重写后的文件 “{label}” 行组数量由 {plan.num_row_groups} 变为 "
                f"{rewritten.num_row_groups}，已中止。",
                files=[label],
            )
        if rewritten.metadata.num_rows != source.metadata.num_rows:
            raise CheckError(
                f"重写后的文件 “{label}” 行数发生变化，已中止。",
                files=[label],
            )
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"无法校验重写后的文件 “{label}”：{exc}", files=[label]
        ) from None

    return FileResult(
        entry=plan.entry,
        output_path=output_path,
        changes=list(plan.changes),
        kept_ids=list(plan.kept_ids),
        num_row_groups=plan.num_row_groups,
    )


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def check_presence(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> None:
    missing = [
        entry.uploaded_filename
        for entry in manifest.required_entries
        if entry.uploaded_filename not in uploaded_paths
        or not Path(uploaded_paths[entry.uploaded_filename]).is_file()
    ]
    if missing:
        raise CheckError(
            "清单中以下文件的上传副本缺失，无法回填字段 ID。",
            files=missing,
        )


def run_backfill(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    name_mapping: NameMapping,
    output_dir: Path,
) -> BackfillResult:
    """Validate every replica first, then rewrite the whole batch.

    ``output_dir`` must be an empty directory reserved for this request.
    Output files keep the manifest's ``uploaded_filename`` so that the ZIP
    can restore the original names.
    """
    check_presence(manifest, uploaded_paths)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: nothing is written until every file passes planning.
    plans: list[FilePlan] = []
    for entry in manifest.required_entries:
        plans.append(
            _plan_file(entry, uploaded_paths[entry.uploaded_filename], name_mapping)
        )

    # Phase 2: all plans are sound; rewrite one file at a time.
    results: list[FileResult] = []
    for plan in plans:
        results.append(_rewrite_file(plan, output_dir))

    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused_uploads = sorted(
        name for name in uploaded_paths if name not in referenced
    )
    warnings: list[str] = []
    if unused_uploads:
        warnings.append(
            "以下上传文件未出现在清单中，已忽略：" + "、".join(unused_uploads)
        )
    if manifest.equality_delete_files:
        warnings.append(
            "清单中的 equality_delete 条目按规则忽略，未要求其副本、也未重写："
            + "、".join(entry.uploaded_filename for entry in manifest.equality_delete_files)
        )

    return BackfillResult(
        manifest=manifest,
        file_results=results,
        unused_uploads=unused_uploads,
        warnings=warnings,
    )
