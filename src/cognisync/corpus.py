from __future__ import annotations

from pathlib import PurePosixPath
from typing import Iterable, List, Optional, Set


REPO_QUERY_TOKENS: Set[str] = {
    "api",
    "branch",
    "class",
    "cli",
    "code",
    "command",
    "commit",
    "function",
    "implementation",
    "interface",
    "module",
    "package",
    "repo",
    "repository",
    "script",
}
PDF_QUERY_TOKENS: Set[str] = {
    "citation",
    "citations",
    "journal",
    "literature",
    "paper",
    "pdf",
    "publication",
    "research",
    "study",
}
VISUAL_QUERY_TOKENS: Set[str] = {
    "architecture",
    "chart",
    "diagram",
    "figure",
    "image",
    "images",
    "screenshot",
    "visual",
    "workflow",
}
WEB_QUERY_TOKENS: Set[str] = {
    "article",
    "blog",
    "docs",
    "documentation",
    "link",
    "page",
    "site",
    "url",
    "web",
    "website",
}


def classify_source_kind(path: str, tags: Iterable[str]) -> str:
    normalized_tags = {tag.lower() for tag in tags}
    if "repo-ingest" in normalized_tags or path.startswith("raw/repos/"):
        return "repo"
    if "pdf-ingest" in normalized_tags or path.startswith("raw/pdfs/"):
        return "pdf"
    if "url-ingest" in normalized_tags or path.startswith("raw/urls/"):
        return "url"
    if path.startswith("raw/files/"):
        return "file"
    if path.startswith("wiki/concepts/"):
        return "concept"
    if path.startswith("wiki/sources/"):
        return "summary"
    if path.startswith("wiki/queries/"):
        return "query"
    if path.startswith("outputs/reports/"):
        return "report"
    if path.startswith("outputs/slides/"):
        return "slide"
    if path.startswith("prompts/"):
        return "prompt"
    return "artifact"


def source_group_key(path: str) -> Optional[str]:
    parts = PurePosixPath(path).parts
    if not parts or parts[0] != "raw":
        return None
    if len(parts) == 2:
        return f"raw/{PurePosixPath(path).stem}"

    category = parts[1]
    if category == "urls" and len(parts) >= 3 and parts[2].endswith("-assets"):
        return f"raw/urls/{parts[2][:-7]}"

    return f"raw/{category}/{PurePosixPath(path).stem}"


def pick_primary_artifact(paths: List[str], source_kind: str) -> Optional[str]:
    preferred_suffixes = {
        "pdf": [".md", ".pdf"],
        "url": [".md"],
        "repo": [".md"],
        "file": [".md", ".txt", ".rst"],
    }.get(source_kind, [".md", ".txt", ".json", ".csv"])

    for suffix in preferred_suffixes:
        for path in paths:
            if path.endswith(suffix):
                return path
    return paths[0] if paths else None


def infer_extraction_status(source_kind: str, paths: List[str]) -> str:
    if source_kind == "pdf":
        return "text_available" if any(path.endswith(".md") for path in paths) else "binary_only"
    if source_kind == "url":
        return "captured" if any(path.endswith(".md") for path in paths) else "assets_only"
    if source_kind == "repo":
        return "manifest_available"
    if source_kind == "file":
        return "copied"
    return "available"


def source_kind_boost(source_kind: str, query_tokens: Iterable[str], image_count: int = 0) -> float:
    token_set = set(query_tokens)
    boost = 1.0
    if token_set & REPO_QUERY_TOKENS and source_kind == "repo":
        boost += 0.35
    if token_set & PDF_QUERY_TOKENS and source_kind == "pdf":
        boost += 0.35
    if token_set & WEB_QUERY_TOKENS and source_kind == "url":
        boost += 0.2
    if token_set & VISUAL_QUERY_TOKENS and (source_kind == "url" or image_count):
        boost += 0.3
    if token_set & VISUAL_QUERY_TOKENS and image_count:
        boost += 0.1
    if source_kind == "concept" and not (token_set & (REPO_QUERY_TOKENS | PDF_QUERY_TOKENS)):
        boost += 0.05
    return boost
