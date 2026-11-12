"""Backfill Iceberg field IDs into historical Parquet replicas.

The input is an existing valid-file manifest, the Parquet copies named by it,
and the JSON value of the table property ``schema.name-mapping.default``.  For
every ``data`` file we add the Iceberg field IDs to the column metadata that
PyArrow writes as Parquet ``field_id`` entries.

Rules:

* Only scalar columns and one level of ``struct`` are supported.
* A field that already carries a field ID is left alone.
* A field without an ID is matched by name/alias against the name mapping.
  The match must be unique (one mapping entry).
* A struct child is matched only inside the mapping branch selected by the
  parent field's ID.  This is what gives two same-named child columns under
  different parents different IDs.
* Field IDs must be unique within one file; this is checked after combining
  existing and newly matched IDs and before anything is written.
* Files are rewritten in row batches.  Column names, types, values and row
  order are preserved.  The manifest and positional-delete files are not
  touched.
* Any error aborts the whole batch, names the file and column(s) involved,
  and leaves no downloadable output.
"""

from __future__ import annotations

import io
import os
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import CheckError
from .manifest import Manifest, ManifestEntry
from .name_mapping import MappingField, NameMapping

FIELD_ID_METADATA_KEY = b"PARQUET:field_id"
_SUPPORTED_COMPRESSIONS = {
    "snappy",
    "gzip",
    "brotli",
    "lz4",
    "zstd",
    "none",
}


@dataclass(frozen=True)
class FieldIdChange:
    path: str
    field_id: int
    previous_id: int | None

    @property
    def added(self) -> bool:
        return self.previous_id is None


@dataclass
class FileBackfill:
    entry: ManifestEntry
    output_path: Path
    changes: list[FieldIdChange]

    @property
    def added_changes(self) -> list[FieldIdChange]:
        return [change for change in self.changes if change.added]

    @property
    def kept_changes(self) -> list[FieldIdChange]:
        return [change for change in self.changes if not change.added]


@dataclass
class BackfillResult:
    files: list[FileBackfill]
    warnings: list[str] = field(default_factory=list)

    @property
    def added_count(self) -> int:
        return sum(len(item.added_changes) for item in self.files)

    @property
    def kept_count(self) -> int:
        return sum(len(item.kept_changes) for item in self.files)


def _field_id(
    field: pa.Field, *, file_label: str, path: str
) -> int | None:
    metadata = field.metadata
    if not metadata:
        return None
    raw = metadata.get(FIELD_ID_METADATA_KEY)
    if raw is None:
        raw = metadata.get(FIELD_ID_METADATA_KEY.decode("ascii"))
    if raw is None:
        return None
    try:
        if isinstance(raw, bytes):
            value = int(raw.decode("ascii").strip(), 10)
        else:
            value = int(str(raw).strip(), 10)
    except (UnicodeDecodeError, TypeError, ValueError):
        raise CheckError(
            f"文件 “{file_label}” 的列 “{path}” 已有字段 ID “{raw}”，"
            "但它不是整数。",
            files=[file_label],
            columns=[path],
        ) from None
    if value <= 0:
        raise CheckError(
            f"文件 “{file_label}” 的列 “{path}” 已有字段 ID {value}，"
            "必须为正整数。",
            files=[file_label],
            columns=[path],
        )
    return value


def _with_field_id(field: pa.Field, field_id: int) -> pa.Field:
    metadata = dict(field.metadata or {})
    metadata[FIELD_ID_METADATA_KEY] = str(field_id).encode("ascii")
    return field.with_metadata(metadata)


def _is_nested(type_: pa.DataType) -> bool:
    return (
        pa.types.is_struct(type_)
        or pa.types.is_list(type_)
        or pa.types.is_large_list(type_)
        or pa.types.is_fixed_size_list(type_)
        or pa.types.is_map(type_)
        or pa.types.is_union(type_)
    )


def _validate_supported_field(
    field: pa.Field, *, path: str, file_label: str
) -> None:
    """Reject nested types beyond a single struct level.

    Scalar columns of every Arrow primitive type are fine.  A struct is fine
    when each child is scalar; a struct of structs, a list, a map, etc. is
    outside the supported subset.
    """
    type_ = field.type
    if pa.types.is_struct(type_):
        for child in type_:
            if _is_nested(child.type):
                raise CheckError(
                    f"文件 “{file_label}” 的列 “{path}.{child.name}” 类型为 "
                    f"{child.type}；只支持标量列和一层 struct。",
                    files=[file_label],
                    columns=[f"{path}.{child.name}"],
                )
        return
    if _is_nested(type_):
        raise CheckError(
            f"文件 “{file_label}” 的列 “{path}” 类型为 {type_}；"
            "只支持标量列和一层 struct。",
            files=[file_label],
            columns=[path],
        )


def _ambiguous(
    *,
    file_label: str,
    path: str,
    candidates: list[MappingField],
    reason: str,
) -> CheckError:
    names = "、".join(f"{c.path}（ID {c.field_id}）" for c in candidates)
    return CheckError(
        f"文件 “{file_label}” 的列 “{path}” {reason}，匹配到多个 "
        f"name mapping 条目：{names}。无法唯一补 ID。",
        files=[file_label],
        columns=[path],
    )


def _resolve_field(
    field: pa.Field,
    *,
    path: str,
    file_label: str,
    mapping: NameMapping,
    parent_node: MappingField | None,
    top_level: bool,
    changes: list[FieldIdChange],
    seen_ids: dict[int, str],
) -> pa.Field:
    """Resolve one field and recurse into a one-level struct.

    ``parent_node`` is the mapping branch found through the parent field's
    ID.  It is the only branch consulted for a child.
    """
    _validate_supported_field(field, path=path, file_label=file_label)
    previous = _field_id(field, file_label=file_label, path=path)

    if previous is not None:
        field_id = previous
        node = mapping.by_id.get(field_id)
    else:
        if top_level:
            candidates = mapping.top_level_matches(field.name)
            missing_note = "在 name mapping 顶层找不到别名匹配"
        else:
            if parent_node is None:
                raise CheckError(
                    f"文件 “{file_label}” 的列 “{path}” 需要补 ID，但无法根"
                    "据父字段的 ID 找到对应的子映射：父字段 ID 不在 "
                    "name mapping 中，或其父字段同样没有可用 ID。",
                    files=[file_label],
                    columns=[path],
                )
            candidates = [
                item for item in parent_node.fields if field.name in item.names
            ]
            missing_note = (
                f"在父字段映射 “{parent_node.path}”（ID "
                f"{parent_node.field_id}）下找不到别名匹配"
            )

        if not candidates:
            raise CheckError(
                f"文件 “{file_label}” 的列 “{path}” {missing_note}；"
                "无法唯一补 ID。",
                files=[file_label],
                columns=[path],
            )
        if len(candidates) > 1:
            raise _ambiguous(
                file_label=file_label,
                path=path,
                candidates=candidates,
                reason="没有唯一的别名匹配",
            )
        node = candidates[0]
        field_id = node.field_id

    owner = seen_ids.get(field_id)
    if owner is not None:
        raise CheckError(
            f"文件 “{file_label}” 内字段 ID {field_id} 重复：列 “{owner}” "
            f"与列 “{path}”。无法写出唯一的 field ID。",
            files=[file_label],
            columns=sorted({owner, path}),
        )
    seen_ids[field_id] = path
    changes.append(FieldIdChange(path=path, field_id=field_id, previous_id=previous))

    if pa.types.is_struct(field.type):
        new_children = [
            _resolve_field(
                child,
                path=f"{path}.{child.name}",
                file_label=file_label,
                mapping=mapping,
                parent_node=node,
                top_level=False,
                changes=changes,
                seen_ids=seen_ids,
            )
            for child in field.type
        ]
        new_type = pa.struct(new_children)
    else:
        new_type = field.type

    return pa.field(
        field.name,
        new_type,
        nullable=field.nullable,
        metadata=_with_field_id(field, field_id).metadata,
    )


def resolve_schema_ids(
    schema: pa.Schema,
    mapping: NameMapping,
    *,
    file_label: str,
) -> tuple[pa.Schema, list[FieldIdChange]]:
    """Return the schema with field IDs filled in and the change list."""
    changes: list[FieldIdChange] = []
    seen_ids: dict[int, str] = {}
    fields = [
        _resolve_field(
            field,
            path=field.name,
            file_label=file_label,
            mapping=mapping,
            parent_node=None,
            top_level=True,
            changes=changes,
            seen_ids=seen_ids,
        )
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata), changes


def _source_compression(metadata: pq.FileMetaData) -> str:
    seen: set[str] = set()
    for group_index in range(metadata.num_row_groups):
        group = metadata.row_group(group_index)
        for column_index in range(group.num_columns):
            raw = str(group.column(column_index).compression).lower()
            seen.add(raw)
    if len(seen) == 1:
        value = seen.pop()
        if value in _SUPPORTED_COMPRESSIONS:
            return value
    return "snappy"


def _verify_output(
    source: pq.ParquetFile,
    output_path: Path,
    target_schema: pa.Schema,
    *,
    file_label: str,
    columns: list[str],
) -> None:
    try:
        written = pq.ParquetFile(str(output_path))
        written_schema = written.schema_arrow
        expected_rows = source.metadata.num_rows
        actual_rows = written.metadata.num_rows
    except Exception as exc:
        raise CheckError(
            f"重写文件 “{file_label}” 后无法读取输出：{exc}",
            files=[file_label],
            columns=columns,
        ) from None
    if expected_rows != actual_rows:
        raise CheckError(
            f"重写文件 “{file_label}” 后行数从 {expected_rows} 变为 "
            f"{actual_rows}，行序或内容可能已改变。",
            files=[file_label],
            columns=columns,
        )
    if len(written_schema) != len(target_schema):
        raise CheckError(
            f"重写文件 “{file_label}” 后列数发生变化。",
            files=[file_label],
            columns=columns,
        )
    for expected, actual in zip(target_schema, written_schema):
        if (
            expected.name != actual.name
            or expected.type != actual.type
            or expected.nullable != actual.nullable
        ):
            raise CheckError(
                f"重写文件 “{file_label}” 后列 “{expected.name}” 的名称或"
                "类型发生变化。",
                files=[file_label],
                columns=[expected.name],
            )


def _rewrite_file(
    entry: ManifestEntry,
    source_path: Path,
    output_path: Path,
    target_schema: pa.Schema,
    *,
    batch_size: int,
) -> None:
    label = entry.uploaded_filename
    columns = [field.name for field in target_schema]
    source = None
    writer = None
    error_path: str | None = None
    try:
        source = pq.ParquetFile(str(source_path))
        compression = _source_compression(source.metadata)
        writer = pq.ParquetWriter(
            str(output_path),
            target_schema,
            compression=compression,
        )
        for batch in source.iter_batches(batch_size=batch_size):
            if batch.num_columns != len(target_schema):
                raise CheckError(
                    f"读取文件 “{label}” 时列数从 "
                    f"{len(target_schema)} 变为 {batch.num_columns}。",
                    files=[label],
                    columns=columns,
                )
            # Same arrays, same order, same types; only the schema metadata
            # now includes the field IDs.
            rebatched = pa.RecordBatch.from_arrays(
                batch.columns, schema=target_schema
            )
            writer.write_batch(rebatched)
        writer.close()
        writer = None
        _verify_output(
            source,
            output_path,
            target_schema,
            file_label=label,
            columns=columns,
        )
    except CheckError as exc:
        error_path = label
        raise
    except Exception as exc:
        error_path = label
        raise CheckError(
            f"重写文件 “{label}”（原始路径 {entry.original_path}）失败：{exc}",
            files=[label],
            columns=columns,
        ) from None
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if source is not None:
            try:
                source.close()
            except Exception:
                pass
        if error_path is not None and output_path.exists():
            # Never leave a partially rewritten replica behind; the batch is
            # all-or-nothing and the caller must not be able to package it.
            try:
                output_path.unlink()
            except OSError:
                pass


def _check_data_presence(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> None:
    missing = [
        entry.uploaded_filename
        for entry in manifest.data_files
        if (
            entry.uploaded_filename not in uploaded_paths
            or not Path(uploaded_paths[entry.uploaded_filename]).is_file()
        )
    ]
    if missing:
        raise CheckError(
            "清单中以下数据文件的上传副本缺失，无法补字段 ID。",
            files=missing,
        )


def backfill_field_ids(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    mapping: NameMapping,
    output_dir: Path,
    *,
    batch_size: int = 65_536,
) -> BackfillResult:
    """Rewrite every data file of ``manifest`` with resolved field IDs.

    Nothing is returned unless *all* files pass validation and are written
    successfully.  The caller is responsible for packaging the output
    directory only after this function returns.
    """
    if not manifest.data_files:
        raise CheckError(
            "清单中没有 data 类型的数据文件，无法补字段 ID。"
        )
    if batch_size <= 0:
        raise CheckError("批量读取行数必须为正整数。")

    _check_data_presence(manifest, uploaded_paths)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Preflight every file before writing any of them.  This makes column
    # matching, duplicate-ID and unsupported-type errors all-or-nothing too.
    plans = []
    for entry in manifest.data_files:
        label = entry.uploaded_filename
        source_path = Path(uploaded_paths[label])
        try:
            parquet = pq.ParquetFile(str(source_path))
            schema = parquet.schema_arrow
        except Exception as exc:
            raise CheckError(
                f"无法读取数据文件 “{label}”（原始路径 "
                f"{entry.original_path}）：{exc}",
                files=[label],
            ) from None
        try:
            target_schema, changes = resolve_schema_ids(
                schema, mapping, file_label=label
            )
        finally:
            try:
                parquet.close()
            except Exception:
                pass
        plans.append((entry, source_path, target_schema, changes))

    files: list[FileBackfill] = []
    for index, plan in enumerate(plans, start=1):
        entry, source_path, target_schema, changes = plan
        output_path = output_dir / f"output-{index:04d}.parquet"
        _rewrite_file(
            entry,
            source_path,
            output_path,
            target_schema,
            batch_size=batch_size,
        )
        files.append(
            FileBackfill(
                entry=entry,
                output_path=output_path,
                changes=changes,
            )
        )

    referenced = {entry.uploaded_filename for entry in manifest.data_files}
    unused = sorted(
        name for name in uploaded_paths if name not in referenced
    )
    warnings: list[str] = []
    if unused:
        warnings.append(
            "以下上传文件不是清单中的数据文件，已忽略："
            + "、".join(unused)
        )
    return BackfillResult(files=files, warnings=warnings)


def package_zip(result: BackfillResult) -> bytes:
    """Package rewritten files under their original uploaded filenames."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in result.files:
            archive.write(
                item.output_path,
                arcname=item.entry.uploaded_filename,
            )
    return buffer.getvalue()
