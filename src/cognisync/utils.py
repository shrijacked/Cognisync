from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os
import re


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "untitled"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def relative_markdown_path(source_file: Path, target_file: Path) -> str:
    return Path(os.path.relpath(str(target_file), start=str(source_file.parent))).as_posix()
