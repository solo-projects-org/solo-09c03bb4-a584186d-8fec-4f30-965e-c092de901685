"""Error type shared by the manifest parser and the check engine.

A ``CheckError`` always names the file(s) that could not be used.  The Flask
layer deliberately renders only this error and never any partial ticket
result, which keeps the "no partial conclusions" rule.
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
        parts = [self.message]
        if self.files:
            parts.append(f"（文件：{', '.join(self.files)}）")
        if self.columns:
            parts.append(f"（涉及列：{', '.join(self.columns)}）")
        return "".join(parts)
