"""Error type shared by the manifest parser and the field-ID backfill engine.

A ``CheckError`` always names the file(s) and column(s) involved.  The Flask
layer deliberately renders only this error and never offers a download,
which keeps the all-or-nothing rule.
"""

from __future__ import annotations


class CheckError(Exception):
    def __init__(
        self,
        message: str,
        *,
        files: list[str] | None = None,
        columns: list[str] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.files = list(files or [])
        self.columns = list(columns or [])

    def __str__(self) -> str:  # pragma: no cover - trivial
        parts = []
        if self.files:
            parts.append(f"文件：{', '.join(self.files)}")
        if self.columns:
            parts.append(f"列：{', '.join(self.columns)}")
        if parts:
            return f"{self.message}（{'；'.join(parts)}）"
        return self.message
