from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
import posixpath
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from cognisync.types import ArtifactRecord, IndexSnapshot, LinkReference
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace
from cognisync.knowledge_surfaces import is_navigation_surface_path


MARKDOWN_EXTENSIONS = {".md", ".markdown"}
TEXT_EXTENSIONS = {".txt", ".rst"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
DATA_EXTENSIONS = {".csv", ".json", ".jsonl"}
CODE_EXTENSIONS = {".py", ".js", ".ts", ".tsx", ".sh", ".yaml", ".yml", ".toml"}

HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)
MARKDOWN_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
WIKI_LINK_RE = re.compile(r"(?<!!)\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]")
IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")
WIKI_IMAGE_RE = re.compile(r"!\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]")
TAG_RE = re.compile(r"(?<!\w)#([A-Za-z0-9_-]+)")


def detect_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in MARKDOWN_EXTENSIONS:
        return "markdown"
    if suffix in TEXT_EXTENSIONS:
        return "text"
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix == ".pdf":
        return "pdf"
    if suffix in DATA_EXTENSIONS:
        return "data"
    if suffix in CODE_EXTENSIONS:
        return "code"
    return "binary"


def scan_workspace(workspace: Workspace) -> IndexSnapshot:
    inventory = _build_inventory(workspace.root)
    stem_index = _build_stem_index(inventory)
    artifacts = [
        _scan_file(workspace, rel_path, abs_path, inventory, stem_index)
        for rel_path, abs_path in sorted(inventory.items())
    ]
    backlinks = _build_backlinks(artifacts, set(inventory))
    return IndexSnapshot(generated_at=utc_timestamp(), artifacts=artifacts, backlinks=backlinks)


def _build_inventory(root: Path) -> Dict[str, Path]:
    inventory: Dict[str, Path] = {}
    allowed_roots = {"raw", "wiki", "outputs", "prompts"}
    ignored_roots = {
        "outputs/reports/change-summaries",
        "outputs/reports/exports",
        "outputs/reports/remediation-jobs",
        "outputs/reports/research-jobs",
        "outputs/reports/review-exports",
        "outputs/reports/review-ui",
        "outputs/reports/sync-bundles",
    }
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if any(rel == ignored or rel.startswith(f"{ignored}/") for ignored in ignored_roots):
            continue
        parts = PurePosixPath(rel).parts
        if not parts or parts[0] not in allowed_roots:
            continue
        inventory[rel] = path
    return inventory


def _build_stem_index(inventory: Dict[str, Path]) -> Dict[str, List[str]]:
    stems: Dict[str, List[str]] = {}
    for rel_path, abs_path in inventory.items():
        if detect_kind(abs_path) != "markdown":
            continue
        stems.setdefault(abs_path.stem.lower(), []).append(rel_path)
    for paths in stems.values():
        paths.sort()
    return stems


def _scan_file(
    workspace: Workspace,
    rel_path: str,
    abs_path: Path,
    inventory: Dict[str, Path],
    stem_index: Dict[str, List[str]],
) -> ArtifactRecord:
    kind = detect_kind(abs_path)
    collection = PurePosixPath(rel_path).parts[0]
    content_hash = hashlib.sha256(abs_path.read_bytes()).hexdigest()
    modified_at = abs_path.stat().st_mtime
    title = abs_path.stem.replace("-", " ").replace("_", " ").title()
    headings: List[str] = []
    tags: List[str] = []
    links: List[LinkReference] = []
    images: List[str] = []
    word_count = 0

    if kind in {"markdown", "text", "data", "code"}:
        text = abs_path.read_text(encoding="utf-8", errors="ignore")
        frontmatter, body = _extract_frontmatter(text)
        title = _extract_title(frontmatter, body, abs_path.stem)
        headings = _extract_headings(body)
        tags = sorted(set(_extract_tags(frontmatter, body)))
        links = _extract_links(body, rel_path, inventory, stem_index)
        images = _extract_images(body)
        word_count = len(re.findall(r"\b\w+\b", body))

    summary_target = None
    if collection == "raw" and kind in {"markdown", "text", "data", "code"}:
        summary_target = f"wiki/sources/{slugify(abs_path.stem)}.md"

    return ArtifactRecord(
        path=rel_path,
        collection=collection,
        kind=kind,
        title=title,
        word_count=word_count,
        headings=headings,
        tags=tags,
        links=links,
        images=images,
        summary_target=summary_target,
        content_hash=content_hash,
        modified_at=modified_at,
    )


def _extract_frontmatter(text: str) -> Tuple[Dict[str, object], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    block = text[4:end]
    body = text[end + 5 :]
    data: Dict[str, object] = {}
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        data[key.strip().lower()] = _parse_frontmatter_value(value.strip())
    return data, body


def _parse_frontmatter_value(value: str):
    cleaned = value.strip().strip("\"'")
    if cleaned.startswith("[") and cleaned.endswith("]"):
        return [item.strip().strip("\"'") for item in cleaned[1:-1].split(",") if item.strip()]
    if "," in cleaned and "://" not in cleaned:
        return [item.strip().strip("\"'") for item in cleaned.split(",") if item.strip()]
    return cleaned


def _extract_title(frontmatter: Dict[str, object], body: str, stem: str) -> str:
    title = frontmatter.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    match = HEADING_RE.search(body)
    if match:
        return match.group(2).strip()
    return stem.replace("-", " ").replace("_", " ").title()


def _extract_headings(body: str) -> List[str]:
    return [match.group(2).strip() for match in HEADING_RE.finditer(body)]


def _extract_tags(frontmatter: Dict[str, object], body: str) -> List[str]:
    tags: List[str] = []
    frontmatter_tags = frontmatter.get("tags")
    if isinstance(frontmatter_tags, str):
        tags.append(frontmatter_tags.strip().lstrip("#").lower())
    elif isinstance(frontmatter_tags, Sequence):
        for item in frontmatter_tags:
            tags.append(str(item).strip().lstrip("#").lower())
    tags.extend(match.group(1).lower() for match in TAG_RE.finditer(body))
    return [tag for tag in tags if tag]


def _extract_links(
    body: str,
    source_path: str,
    inventory: Dict[str, Path],
    stem_index: Dict[str, List[str]],
) -> List[LinkReference]:
    raw_links: List[Tuple[str, str]] = []
    raw_links.extend((match.group(1).strip(), "markdown_link") for match in MARKDOWN_LINK_RE.finditer(body))
    raw_links.extend((match.group(1).strip(), "wikilink") for match in WIKI_LINK_RE.finditer(body))
    links: List[LinkReference] = []
    for raw_target, link_kind in raw_links:
        links.append(_resolve_link(raw_target, source_path, inventory, stem_index, link_kind))
    return links


def _extract_images(body: str) -> List[str]:
    images = [match.group(1).strip() for match in IMAGE_RE.finditer(body)]
    images.extend(match.group(1).strip() for match in WIKI_IMAGE_RE.finditer(body))
    return images


def _resolve_link(
    raw_target: str,
    source_path: str,
    inventory: Dict[str, Path],
    stem_index: Dict[str, List[str]],
    link_kind: str,
) -> LinkReference:
    target = raw_target.strip()
    if not target:
        return LinkReference(raw_target=raw_target, resolved_path=None, external=False, kind=link_kind)
    if target.startswith(("http://", "https://", "mailto:", "data:")):
        return LinkReference(raw_target=raw_target, resolved_path=None, external=True, kind=link_kind)
    if target.startswith("#"):
        return LinkReference(raw_target=raw_target, resolved_path=source_path, external=False, kind=link_kind)

    clean_target = target.split("#", 1)[0].split("?", 1)[0].strip()
    if not clean_target:
        return LinkReference(raw_target=raw_target, resolved_path=source_path, external=False, kind=link_kind)

    source_dir = posixpath.dirname(source_path)

    candidates: List[str] = []
    if clean_target.startswith("/"):
        candidates.append(clean_target.lstrip("/"))
    if clean_target.startswith(("raw/", "wiki/", "outputs/", "prompts/")):
        candidates.append(posixpath.normpath(clean_target))
    candidates.append(posixpath.normpath(posixpath.join(source_dir, clean_target)))
    if not PurePosixPath(clean_target).suffix:
        candidates.append(posixpath.normpath(posixpath.join(source_dir, clean_target + ".md")))
        candidates.append(f"wiki/concepts/{slugify(PurePosixPath(clean_target).name)}.md")
        candidates.append(f"wiki/sources/{slugify(PurePosixPath(clean_target).name)}.md")
        candidates.append(f"wiki/queries/{slugify(PurePosixPath(clean_target).name)}.md")
        stem_matches = stem_index.get(PurePosixPath(clean_target).name.lower(), [])
        candidates.extend(stem_matches)

    normalized_candidates = []
    seen = set()
    for candidate in candidates:
        normalized = posixpath.normpath(candidate)
        if normalized not in seen:
            seen.add(normalized)
            normalized_candidates.append(normalized)

    for candidate in normalized_candidates:
        if candidate in inventory:
            return LinkReference(raw_target=raw_target, resolved_path=candidate, external=False, kind=link_kind)

    fallback = normalized_candidates[0] if normalized_candidates else None
    return LinkReference(raw_target=raw_target, resolved_path=fallback, external=False, kind=link_kind)


def _build_backlinks(artifacts: Iterable[ArtifactRecord], existing_paths: set) -> Dict[str, List[str]]:
    backlinks: Dict[str, List[str]] = {}
    for artifact in artifacts:
        for link in artifact.links:
            if link.external or not link.resolved_path:
                continue
            if (
                is_navigation_surface_path(artifact.path)
                and artifact.path not in {"wiki/sources.md", "wiki/concepts.md"}
                and link.kind != "wikilink"
            ):
                continue
            if link.resolved_path not in existing_paths:
                continue
            if link.resolved_path == artifact.path:
                continue
            backlinks.setdefault(link.resolved_path, []).append(artifact.path)

    for target, sources in list(backlinks.items()):
        backlinks[target] = sorted(set(sources))
    return backlinks
