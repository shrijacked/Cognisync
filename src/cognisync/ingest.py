from __future__ import annotations

import base64
import binascii
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import mimetypes
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote_to_bytes, urljoin, urlparse
from urllib.request import urlopen
from xml.etree import ElementTree

from cognisync.utils import slugify
from cognisync.workspace import Workspace


class IngestError(RuntimeError):
    pass


NOTEBOOK_EXTENSIONS = {".ipynb"}
DATASET_DESCRIPTOR_EXTENSIONS = {".csv", ".json", ".jsonl", ".md", ".markdown", ".tsv", ".txt"}
IMAGE_FOLDER_EXTENSIONS = {".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"}
MAX_NOTEBOOK_CODE_LINES = 40
MAX_DATASET_PREVIEW_ROWS = 5


@dataclass(frozen=True)
class IngestResult:
    path: Path
    kind: str


@dataclass(frozen=True)
class BatchIngestEntry:
    kind: str
    source: str
    name: Optional[str] = None


@dataclass(frozen=True)
class UrlListEntry:
    url: str
    name: Optional[str] = None


@dataclass(frozen=True)
class HtmlCapture:
    title: str
    description: str
    canonical_url: str
    body_markdown: str
    headings: List[str]
    links: List[str]
    images: List["HtmlImageReference"]
    word_count: int


@dataclass(frozen=True)
class HtmlImageReference:
    source: str
    alt_text: str


@dataclass(frozen=True)
class CapturedImage:
    source: str
    local_markdown_path: str
    filename: str
    alt_text: str


@dataclass(frozen=True)
class PdfCapture:
    page_count: int
    extracted_text: str
    extractor: str


@dataclass(frozen=True)
class NotebookCellSummary:
    cell_type: str
    source: str
    execution_count: Optional[int]
    output_types: List[str]


@dataclass(frozen=True)
class DatasetDescriptorCapture:
    descriptor_type: str
    summary_lines: List[str]
    preview_lines: List[str]


@dataclass(frozen=True)
class ImageFolderItem:
    filename: str
    relative_path: str
    extension: str
    byte_size: int
    caption: str


class _HtmlToMarkdownParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self.description = ""
        self.canonical_url = ""
        self._current_tag: Optional[str] = None
        self._buffer: List[str] = []
        self.blocks: List[str] = []
        self.headings: List[str] = []
        self.links: List[str] = []
        self.images: List[HtmlImageReference] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        attrs_dict = {str(key).lower(): str(value) for key, value in attrs}
        if tag == "meta":
            name = attrs_dict.get("name", "").lower()
            prop = attrs_dict.get("property", "").lower()
            content = attrs_dict.get("content", "").strip()
            if content and (name == "description" or prop == "og:description") and not self.description:
                self.description = content
            return
        if tag == "link":
            rel = attrs_dict.get("rel", "").lower()
            href = attrs_dict.get("href", "").strip()
            if href and "canonical" in rel and not self.canonical_url:
                self.canonical_url = href
            return
        if tag == "a":
            href = attrs_dict.get("href", "").strip()
            if href:
                self.links.append(href)
        if tag == "img":
            src = attrs_dict.get("src", "").strip()
            alt = attrs_dict.get("alt", "").strip()
            if src:
                self.images.append(HtmlImageReference(source=src, alt_text=alt))
            return
        if tag in {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "title"}:
            self._flush()
            self._current_tag = tag

    def handle_endtag(self, tag: str) -> None:
        if self._current_tag == tag:
            text = " ".join(" ".join(self._buffer).split()).strip()
            self._buffer = []
            if not text:
                self._current_tag = None
                return
            if tag == "title":
                self.title = text
            elif tag.startswith("h") and len(tag) == 2 and tag[1].isdigit():
                level = int(tag[1])
                self.blocks.append(f"{'#' * level} {text}")
                self.headings.append(text)
            elif tag == "li":
                self.blocks.append(f"- {text}")
            else:
                self.blocks.append(text)
            self._current_tag = None

    def handle_data(self, data: str) -> None:
        if self._current_tag:
            self._buffer.append(data)

    def _flush(self) -> None:
        self._buffer = []
        self._current_tag = None


def ingest_file(workspace: Workspace, source: Path, category: str = "files", name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_path = Path(source).resolve()
    if not source_path.is_file():
        raise IngestError(f"Source file does not exist: {source_path}")

    target_dir = workspace.raw_dir / category
    target_dir.mkdir(parents=True, exist_ok=True)
    target_name = name or source_path.name
    target_path = target_dir / target_name
    if target_path.exists() and not force:
        raise IngestError(f"Target already exists: {target_path}. Re-run with --force to overwrite it.")

    shutil.copy2(source_path, target_path)
    return IngestResult(path=target_path, kind=category)


def ingest_pdf(workspace: Workspace, source: Path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_path = Path(source).resolve()
    if not source_path.is_file():
        raise IngestError(f"Source file does not exist: {source_path}")

    target_dir = workspace.raw_dir / "pdfs"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_name = name or source_path.name
    target_path = target_dir / target_name
    sidecar_path = target_path.with_suffix(".md")
    if (target_path.exists() or sidecar_path.exists()) and not force:
        raise IngestError(
            f"Target already exists: {target_path} or {sidecar_path}. Re-run with --force to overwrite it."
        )

    shutil.copy2(source_path, target_path)
    capture = _extract_pdf_capture(target_path)
    sidecar_lines = [
        "---",
        f"title: {target_path.stem}",
        "tags: [pdf-ingest]",
        f"source_file: {target_path.name}",
        f"source_path: {target_path}",
        f"page_count: {capture.page_count}",
        f"text_extractor: {capture.extractor}",
        "---",
        f"# {target_path.stem}",
        "",
        "## Extracted Metadata",
        "",
        f"- Source file: `{target_path.name}`",
        f"- Page count: `{capture.page_count}`",
        f"- Text extractor: `{capture.extractor}`",
        f"- Character count: `{len(capture.extracted_text)}`",
        "",
        "## Extracted Text",
        "",
        capture.extracted_text or "No extractable text found.",
        "",
    ]
    sidecar_path.write_text("\n".join(sidecar_lines), encoding="utf-8")
    return IngestResult(path=target_path, kind="pdf")


def ingest_notebook(workspace: Workspace, source: Path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_path = Path(source).resolve()
    if not source_path.is_file():
        raise IngestError(f"Notebook source does not exist: {source_path}")
    if source_path.suffix.lower() not in NOTEBOOK_EXTENSIONS:
        raise IngestError(f"Notebook ingest expects a .ipynb file: {source_path}")

    target_dir = workspace.raw_dir / "notebooks"
    target_dir.mkdir(parents=True, exist_ok=True)
    slug = slugify(name or source_path.stem)
    target_path = target_dir / f"{slug}.ipynb"
    sidecar_path = target_dir / f"{slug}.md"
    if (target_path.exists() or sidecar_path.exists()) and not force:
        raise IngestError(
            f"Target already exists: {target_path} or {sidecar_path}. Re-run with --force to overwrite it."
        )

    shutil.copy2(source_path, target_path)
    try:
        notebook_payload = json.loads(source_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise IngestError(f"Notebook is not valid JSON: {source_path}") from error

    cells = _summarize_notebook_cells(notebook_payload)
    metadata = dict(notebook_payload.get("metadata", {})) if isinstance(notebook_payload.get("metadata", {}), dict) else {}
    kernelspec = dict(metadata.get("kernelspec", {})) if isinstance(metadata.get("kernelspec", {}), dict) else {}
    language_info = dict(metadata.get("language_info", {})) if isinstance(metadata.get("language_info", {}), dict) else {}
    markdown_count = sum(1 for cell in cells if cell.cell_type == "markdown")
    code_count = sum(1 for cell in cells if cell.cell_type == "code")
    raw_count = sum(1 for cell in cells if cell.cell_type == "raw")
    output_count = sum(len(cell.output_types) for cell in cells)
    language = str(language_info.get("name", "") or kernelspec.get("language", "") or "").strip()
    kernel = str(kernelspec.get("display_name", "") or kernelspec.get("name", "") or "").strip()

    lines = [
        "---",
        f"title: {source_path.stem}",
        "tags: [notebook-ingest]",
        f"source_file: {source_path.name}",
        f"source_path: {source_path}",
        f"notebook_file: {target_path.name}",
        "---",
        f"# {source_path.stem}",
        "",
        "## Notebook Metadata",
        "",
        f"- Source file: `{source_path.name}`",
        f"- Notebook file: `{target_path.name}`",
        f"- Cell count: `{len(cells)}`",
        f"- Markdown cells: `{markdown_count}`",
        f"- Code cells: `{code_count}`",
        f"- Raw cells: `{raw_count}`",
        f"- Output count: `{output_count}`",
    ]
    if kernel:
        lines.append(f"- Kernel: `{kernel}`")
    if language:
        lines.append(f"- Language: `{language}`")
    lines.extend(["", "## Cells", ""])
    if not cells:
        lines.extend(["No notebook cells were found.", ""])
    for index, cell in enumerate(cells, start=1):
        lines.extend([f"### Cell {index}: {cell.cell_type}", ""])
        if cell.execution_count is not None:
            lines.extend([f"- Execution count: `{cell.execution_count}`", ""])
        if cell.output_types:
            lines.append(f"- Output count: `{len(cell.output_types)}`")
            lines.append(f"- Output types: {', '.join(f'`{kind}`' for kind in cell.output_types)}")
            lines.append("")
        if cell.cell_type == "code":
            fence_language = language or "text"
            preview = "\n".join(cell.source.splitlines()[:MAX_NOTEBOOK_CODE_LINES])
            if len(cell.source.splitlines()) > MAX_NOTEBOOK_CODE_LINES:
                preview += "\n# ... truncated ..."
            lines.extend([f"```{fence_language}", preview, "```", ""])
        else:
            lines.extend([cell.source or "(empty cell)", ""])

    sidecar_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return IngestResult(path=target_path, kind="notebook")


def ingest_dataset(workspace: Workspace, source: Path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_path = Path(source).resolve()
    if not source_path.is_file():
        raise IngestError(f"Dataset descriptor does not exist: {source_path}")
    suffix = source_path.suffix.lower()
    if suffix not in DATASET_DESCRIPTOR_EXTENSIONS:
        raise IngestError(
            f"Dataset ingest expects a descriptor file with one of: {', '.join(sorted(DATASET_DESCRIPTOR_EXTENSIONS))}."
        )

    target_dir = workspace.raw_dir / "datasets"
    target_dir.mkdir(parents=True, exist_ok=True)
    slug = slugify(name or source_path.stem)
    sidecar_path = target_dir / f"{slug}.md"
    target_asset_name = f"{slug}{suffix}" if suffix not in {".md", ".markdown"} else f"{slug}-descriptor{suffix}"
    target_path = target_dir / target_asset_name
    if (target_path.exists() or sidecar_path.exists()) and not force:
        raise IngestError(
            f"Target already exists: {target_path} or {sidecar_path}. Re-run with --force to overwrite it."
        )

    shutil.copy2(source_path, target_path)
    capture = _capture_dataset_descriptor(source_path)
    lines = [
        "---",
        f"title: {source_path.stem}",
        "tags: [dataset-ingest]",
        f"source_file: {source_path.name}",
        f"source_path: {source_path}",
        f"descriptor_file: {target_path.name}",
        f"descriptor_type: {capture.descriptor_type}",
        "---",
        f"# {source_path.stem}",
        "",
        "## Dataset Descriptor",
        "",
        "This is descriptor-level ingest. Cognisync records metadata and lightweight previews, not full dataset row materialization.",
        "",
        f"- Source file: `{source_path.name}`",
        f"- Descriptor file: `{target_path.name}`",
        f"- Descriptor type: `{capture.descriptor_type}`",
    ]
    lines.extend(capture.summary_lines)
    if capture.preview_lines:
        lines.extend(["", "## Preview", ""])
        lines.extend(capture.preview_lines)
    sidecar_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return IngestResult(path=target_path, kind="dataset")


def ingest_image_folder(workspace: Workspace, source: Path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_dir = Path(source).resolve()
    if not source_dir.is_dir():
        raise IngestError(f"Image folder does not exist: {source_dir}")

    image_paths = sorted(
        [path for path in source_dir.iterdir() if path.is_file() and path.suffix.lower() in IMAGE_FOLDER_EXTENSIONS],
        key=lambda path: path.name.lower(),
    )
    if not image_paths:
        raise IngestError(f"No supported image files found in: {source_dir}")

    target_dir = workspace.raw_dir / "images"
    target_dir.mkdir(parents=True, exist_ok=True)
    slug = slugify(name or source_dir.name)
    sidecar_path = target_dir / f"{slug}.md"
    asset_dir = target_dir / f"{slug}-assets"
    if (sidecar_path.exists() or asset_dir.exists()) and not force:
        raise IngestError(
            f"Target already exists: {sidecar_path} or {asset_dir}. Re-run with --force to overwrite it."
        )
    if force and asset_dir.exists():
        shutil.rmtree(asset_dir)
    asset_dir.mkdir(parents=True, exist_ok=True)

    items: List[ImageFolderItem] = []
    for image_path in image_paths:
        target_path = asset_dir / image_path.name
        shutil.copy2(image_path, target_path)
        caption = _read_image_caption(image_path)
        items.append(
            ImageFolderItem(
                filename=image_path.name,
                relative_path=f"{asset_dir.name}/{image_path.name}",
                extension=image_path.suffix.lower().lstrip("."),
                byte_size=image_path.stat().st_size,
                caption=caption,
            )
        )

    extension_counts: Dict[str, int] = {}
    for item in items:
        extension_counts[item.extension] = extension_counts.get(item.extension, 0) + 1

    lines = [
        "---",
        f"title: {source_dir.name}",
        "tags: [image-folder-ingest]",
        f"source_folder: {source_dir}",
        f"asset_folder: {asset_dir.name}",
        "---",
        f"# {source_dir.name}",
        "",
        "## Image Folder Metadata",
        "",
        f"- Source folder: `{source_dir}`",
        f"- Asset folder: `{asset_dir.name}`",
        f"- Image count: `{len(items)}`",
        "",
        "## Extension Counts",
        "",
    ]
    for extension, count in sorted(extension_counts.items()):
        lines.append(f"- `{extension}`: {count}")
    lines.extend(["", "## Images", ""])
    for item in items:
        lines.extend(
            [
                f"### {item.filename}",
                "",
                f"![{item.filename}]({item.relative_path})",
                "",
                f"- File: `{item.relative_path}`",
                f"- Extension: `{item.extension}`",
                f"- Byte size: `{item.byte_size}`",
            ]
        )
        if item.caption:
            lines.extend(["", "Caption:", "", item.caption])
        lines.append("")

    sidecar_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return IngestResult(path=sidecar_path, kind="image_folder")


def ingest_url(workspace: Workspace, url: str, name: Optional[str] = None, force: bool = False) -> IngestResult:
    target_dir = workspace.raw_dir / "urls"
    target_dir.mkdir(parents=True, exist_ok=True)

    with urlopen(url) as response:  # nosec B310 - intentional CLI fetch helper
        raw_bytes = response.read()
        content_type = response.headers.get_content_type() if response.headers else "application/octet-stream"
        charset = response.headers.get_content_charset() if response.headers else "utf-8"

    text = raw_bytes.decode(charset or "utf-8", errors="ignore")
    capture = _convert_remote_text_to_markdown(text=text, content_type=content_type)
    title = capture.title or "Web Capture"
    slug = slugify(name or title or _slug_from_url(url))
    target_path = target_dir / f"{slug}.md"
    if target_path.exists() and not force:
        raise IngestError(f"Target already exists: {target_path}. Re-run with --force to overwrite it.")
    captured_images = _capture_url_images(
        workspace=workspace,
        page_url=url,
        slug=slug,
        images=capture.images,
        force=force,
    )

    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    frontmatter = [
        "---",
        f"title: {title or slug}",
        "tags: [url-ingest]",
        f"source_url: {url}",
        f"fetched_at: {fetched_at}",
        f"content_type: {content_type}",
    ]
    if capture.description:
        frontmatter.append(f"description: {capture.description}")
    if capture.canonical_url:
        frontmatter.append(f"canonical_url: {capture.canonical_url}")
    frontmatter.append("---")

    metadata_lines = [
        "## Extracted Metadata",
        "",
        f"- Content type: `{content_type}`",
        f"- Heading count: `{len(capture.headings)}`",
        f"- Outbound link count: `{len(capture.links)}`",
        f"- Captured image count: `{len(captured_images)}`",
        f"- Word count: `{capture.word_count}`",
        "",
    ]
    if capture.description:
        metadata_lines.extend(["## Description", "", capture.description, ""])
    if capture.headings:
        metadata_lines.extend(["## Headings", ""])
        for heading in capture.headings:
            metadata_lines.append(f"- {heading}")
        metadata_lines.append("")
    if capture.links:
        metadata_lines.extend(["## Discovered Links", ""])
        for link in capture.links[:20]:
            metadata_lines.append(f"- {link}")
        metadata_lines.append("")
    if captured_images:
        metadata_lines.extend(["## Captured Images", ""])
        for image in captured_images:
            alt_label = image.alt_text or image.filename
            metadata_lines.append(f"- ![{alt_label}]({image.local_markdown_path})")
        metadata_lines.append("")

    target_path.write_text(
        "\n".join(
            frontmatter
            + [
                f"# {title or slug}",
                "",
                f"Source URL: {url}",
                f"Fetched: {fetched_at}",
                "",
            ]
            + metadata_lines
            + [
                "## Content",
                "",
                capture.body_markdown.strip(),
                "",
            ]
        ),
        encoding="utf-8",
    )
    return IngestResult(path=target_path, kind="url")


def ingest_repo(workspace: Workspace, repo_path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_dir, cleanup_dir, source_repo = _prepare_repo_checkout(repo_path)
    repo_name = name or _repo_name_from_source(repo_path, source_dir)
    target_dir = workspace.raw_dir / "repos"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{slugify(repo_name)}.md"
    if target_path.exists() and not force:
        raise IngestError(f"Target already exists: {target_path}. Re-run with --force to overwrite it.")

    try:
        branch = _git_output(source_dir, ["git", "branch", "--show-current"])
        commit = _git_output(source_dir, ["git", "rev-parse", "--short", "HEAD"])
        remote = _git_output(source_dir, ["git", "remote", "get-url", "origin"]) or source_repo or ""
        recent_commits = _git_output_lines(source_dir, ["git", "log", "--pretty=format:%h %s", "-n", "5"])

        top_level = sorted(path.name for path in source_dir.iterdir() if path.name != ".git")
        repo_files = [
            path
            for path in source_dir.rglob("*")
            if path.is_file() and ".git" not in path.parts
        ]
        directory_count = len(
            {
                path.parent.relative_to(source_dir).as_posix()
                for path in repo_files
                if path.parent != source_dir
            }
        )
        file_count = len(repo_files)
        languages = _language_counts(repo_files)
        tree_snapshot = _render_repository_tree(source_dir)
        readme_path = _find_readme(source_dir)
        readme_excerpt = ""
        if readme_path:
            readme_excerpt = "\n".join(readme_path.read_text(encoding="utf-8", errors="ignore").splitlines()[:12]).strip()

        lines = [
            "---",
            f"title: {repo_name}",
            "tags: [repo-ingest]",
            f"source_path: {source_dir}",
        ]
        if source_repo:
            lines.append(f"source_repo: {source_repo}")
        if remote:
            lines.append(f"origin_remote: {remote}")
        if branch:
            lines.append(f"current_branch: {branch}")
        if commit:
            lines.append(f"current_commit: {commit}")
        lines.extend(
            [
                "---",
                f"# {repo_name}",
                "",
                f"Source path: `{source_dir}`",
                "",
            ]
        )
        if source_repo:
            lines.extend([f"Source repo: `{source_repo}`", ""])
        lines.extend(
            [
                "## Repository Stats",
                "",
                f"- File count: `{file_count}`",
                f"- Directory count: `{directory_count}`",
            ]
        )
        if branch:
            lines.append(f"- Current branch: `{branch}`")
        if commit:
            lines.append(f"- Current commit: `{commit}`")
        if remote:
            lines.append(f"- Origin remote: `{remote}`")
        lines.extend(["", "## Top-level tree", ""])
        for entry in top_level[:30]:
            lines.append(f"- `{entry}`")
        if languages:
            lines.extend(["", "## Language Signals", ""])
            for language, count in languages:
                lines.append(f"- `{language}`: {count} file(s)")
        if recent_commits:
            lines.extend(["", "## Recent Commits", ""])
            for commit_line in recent_commits:
                lines.append(f"- {commit_line}")
        if tree_snapshot:
            lines.extend(["", "## Repository Tree Snapshot", "", "```text"])
            lines.extend(tree_snapshot)
            lines.extend(["```"])
        if readme_excerpt:
            lines.extend(["", "## README excerpt", "", readme_excerpt, ""])

        target_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    finally:
        if cleanup_dir is not None:
            cleanup_dir.cleanup()
    return IngestResult(path=target_path, kind="repo")


def ingest_batch(workspace: Workspace, manifest_path: Path, force: bool = False) -> List[IngestResult]:
    entries = _load_batch_manifest(Path(manifest_path))
    results: List[IngestResult] = []
    for entry in entries:
        if entry.kind == "file":
            results.append(ingest_file(workspace, source=Path(entry.source), category="files", name=entry.name, force=force))
        elif entry.kind == "pdf":
            results.append(ingest_pdf(workspace, source=Path(entry.source), name=entry.name, force=force))
        elif entry.kind == "notebook":
            results.append(ingest_notebook(workspace, source=Path(entry.source), name=entry.name, force=force))
        elif entry.kind == "dataset":
            results.append(ingest_dataset(workspace, source=Path(entry.source), name=entry.name, force=force))
        elif entry.kind in {"image-folder", "image_folder", "images"}:
            results.append(ingest_image_folder(workspace, source=Path(entry.source), name=entry.name, force=force))
        elif entry.kind == "url":
            results.append(ingest_url(workspace, url=entry.source, name=entry.name, force=force))
        elif entry.kind in {"urls", "url-list", "url_list"}:
            results.extend(ingest_urls(workspace, source_list=Path(entry.source), force=force))
        elif entry.kind == "repo":
            results.append(ingest_repo(workspace, repo_path=entry.source, name=entry.name, force=force))
        elif entry.kind == "sitemap":
            results.extend(ingest_sitemap(workspace, source=entry.source, force=force))
        else:
            raise IngestError(
                "Unsupported batch ingest kind "
                f"'{entry.kind}'. Expected one of file, pdf, notebook, dataset, image-folder, url, urls, sitemap, repo."
            )
    return results


def ingest_urls(workspace: Workspace, source_list: Path, force: bool = False) -> List[IngestResult]:
    entries = _load_url_list_entries(Path(source_list))
    results: List[IngestResult] = []
    for entry in entries:
        results.append(ingest_url(workspace, url=entry.url, name=entry.name, force=force))
    return results


def ingest_sitemap(workspace: Workspace, source: str, force: bool = False, limit: Optional[int] = None) -> List[IngestResult]:
    urls = _load_sitemap_urls(source)
    if limit is not None:
        urls = urls[:limit]
    results: List[IngestResult] = []
    for url in urls:
        results.append(ingest_url(workspace, url=url, force=force))
    return results


def _summarize_notebook_cells(notebook_payload: Dict[str, object]) -> List[NotebookCellSummary]:
    raw_cells = notebook_payload.get("cells", [])
    if not isinstance(raw_cells, list):
        raise IngestError("Notebook JSON must include a `cells` list.")
    cells: List[NotebookCellSummary] = []
    for raw_cell in raw_cells:
        if not isinstance(raw_cell, dict):
            continue
        cell_type = str(raw_cell.get("cell_type", "unknown")).strip() or "unknown"
        outputs = raw_cell.get("outputs", [])
        output_types = []
        if isinstance(outputs, list):
            for output in outputs:
                if isinstance(output, dict):
                    output_type = str(output.get("output_type", "")).strip()
                    if output_type:
                        output_types.append(output_type)
        execution_count = raw_cell.get("execution_count")
        cells.append(
            NotebookCellSummary(
                cell_type=cell_type,
                source=_cell_source_text(raw_cell.get("source", "")),
                execution_count=execution_count if isinstance(execution_count, int) else None,
                output_types=output_types,
            )
        )
    return cells


def _cell_source_text(source: object) -> str:
    if isinstance(source, list):
        return "".join(str(part) for part in source).strip()
    return str(source or "").strip()


def _capture_dataset_descriptor(source_path: Path) -> DatasetDescriptorCapture:
    suffix = source_path.suffix.lower()
    text = source_path.read_text(encoding="utf-8", errors="ignore")
    if suffix == ".json":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as error:
            raise IngestError(f"Dataset descriptor is not valid JSON: {source_path}") from error
        summary_lines = _json_descriptor_summary_lines(payload)
        preview = ["```json", json.dumps(payload, indent=2, sort_keys=True)[:2000], "```"]
        return DatasetDescriptorCapture(descriptor_type="json", summary_lines=summary_lines, preview_lines=preview)
    if suffix == ".jsonl":
        lines = [line for line in text.splitlines() if line.strip()]
        preview_items = []
        for line in lines[:MAX_DATASET_PREVIEW_ROWS]:
            try:
                preview_items.append(json.loads(line))
            except json.JSONDecodeError:
                preview_items.append(line)
        summary_lines = [
            f"- JSONL records detected: `{len(lines)}`",
            f"- Rows previewed: `{len(preview_items)}`",
        ]
        preview = ["```json", json.dumps(preview_items, indent=2, sort_keys=True), "```"]
        return DatasetDescriptorCapture(descriptor_type="jsonl", summary_lines=summary_lines, preview_lines=preview)
    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        rows = list(csv.reader(text.splitlines(), delimiter=delimiter))
        header = rows[0] if rows else []
        preview_rows = rows[1 : MAX_DATASET_PREVIEW_ROWS + 1] if len(rows) > 1 else []
        summary_lines = [
            f"- Column count: `{len(header)}`",
            f"- Rows previewed: `{len(preview_rows)}`",
        ]
        if header:
            summary_lines.append(f"- Columns: {', '.join(f'`{column}`' for column in header)}")
        preview = _markdown_table(header, preview_rows)
        return DatasetDescriptorCapture(
            descriptor_type="tsv" if suffix == ".tsv" else "csv",
            summary_lines=summary_lines,
            preview_lines=preview,
        )
    excerpt = "\n".join(text.splitlines()[:80]).strip()
    return DatasetDescriptorCapture(
        descriptor_type=suffix.lstrip(".") or "text",
        summary_lines=[f"- Character count: `{len(text)}`"],
        preview_lines=["```text", excerpt or "(empty descriptor)", "```"],
    )


def _json_descriptor_summary_lines(payload: object) -> List[str]:
    if isinstance(payload, dict):
        lines = [f"- Top-level key count: `{len(payload)}`", "", "### Top-Level Keys", ""]
        for key in sorted(payload):
            value = payload[key]
            value_type = type(value).__name__
            lines.append(f"- `{key}` ({value_type})")
        return lines
    if isinstance(payload, list):
        return [f"- Top-level list items: `{len(payload)}`"]
    return [f"- Top-level value type: `{type(payload).__name__}`"]


def _markdown_table(header: List[str], rows: List[List[str]]) -> List[str]:
    if not header:
        return ["No tabular preview rows found."]
    normalized_header = [str(column) for column in header]
    lines = [
        "| " + " | ".join(normalized_header) + " |",
        "| " + " | ".join("---" for _ in normalized_header) + " |",
    ]
    for row in rows:
        padded = [str(value) for value in row[: len(normalized_header)]]
        while len(padded) < len(normalized_header):
            padded.append("")
        lines.append("| " + " | ".join(padded) + " |")
    return lines


def _read_image_caption(image_path: Path) -> str:
    for suffix in [".md", ".txt"]:
        caption_path = image_path.with_suffix(suffix)
        if caption_path.exists() and caption_path.is_file():
            return caption_path.read_text(encoding="utf-8", errors="ignore").strip()
    return ""


def _convert_remote_text_to_markdown(text: str, content_type: str) -> HtmlCapture:
    if "html" in content_type:
        parser = _HtmlToMarkdownParser()
        parser.feed(text)
        body = "\n\n".join(block for block in parser.blocks if block)
        word_count = len((body or text).split())
        return HtmlCapture(
            title=parser.title or "Web Capture",
            description=parser.description,
            canonical_url=parser.canonical_url,
            body_markdown=body or text,
            headings=parser.headings,
            links=_dedupe_preserve_order(parser.links),
            images=parser.images,
            word_count=word_count,
        )
    if "json" in content_type:
        parsed = json.loads(text)
        body = "```json\n" + json.dumps(parsed, indent=2, sort_keys=True) + "\n```"
        return HtmlCapture(
            title="JSON Capture",
            description="",
            canonical_url="",
            body_markdown=body,
            headings=[],
            links=[],
            images=[],
            word_count=len(body.split()),
        )
    return HtmlCapture(
        title="Web Capture",
        description="",
        canonical_url="",
        body_markdown=text,
        headings=[],
        links=[],
        images=[],
        word_count=len(text.split()),
    )


def _extract_pdf_capture(pdf_path: Path) -> PdfCapture:
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        pages = len(reader.pages)
        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            page_text = page_text.strip()
            if page_text:
                text_parts.append(page_text)
        extracted = "\n\n".join(text_parts).strip()
        if extracted:
            return PdfCapture(page_count=pages, extracted_text=extracted, extractor="pypdf")
    except Exception:
        pass

    raw = pdf_path.read_bytes()
    page_count = max(1, len(re.findall(rb"/Type\s*/Page\b", raw)))
    extracted = _extract_pdf_text_fallback(raw)
    return PdfCapture(
        page_count=page_count,
        extracted_text=extracted or "No extractable text found.",
        extractor="basic",
    )


def _extract_pdf_text_fallback(raw: bytes) -> str:
    text_parts: List[str] = []
    for stream in re.findall(rb"stream\r?\n(.*?)\r?\nendstream", raw, flags=re.DOTALL):
        for match in re.finditer(rb"\((?:\\.|[^\\)])*\)\s*Tj", stream):
            literal = match.group(0).rsplit(b")", 1)[0][1:]
            decoded = _decode_pdf_literal(literal)
            if decoded:
                text_parts.append(decoded)
        for array_match in re.finditer(rb"\[(.*?)\]\s*TJ", stream, flags=re.DOTALL):
            chunks = re.findall(rb"\((?:\\.|[^\\)])*\)", array_match.group(1))
            decoded_chunks = [_decode_pdf_literal(chunk[1:-1]) for chunk in chunks]
            joined = "".join(chunk for chunk in decoded_chunks if chunk)
            if joined:
                text_parts.append(joined)
    return "\n\n".join(part.strip() for part in text_parts if part.strip())


def _decode_pdf_literal(raw: bytes) -> str:
    decoded = bytearray()
    index = 0
    while index < len(raw):
        byte = raw[index]
        if byte != 0x5C:
            decoded.append(byte)
            index += 1
            continue
        index += 1
        if index >= len(raw):
            break
        escaped = raw[index]
        if escaped in {0x28, 0x29, 0x5C}:
            decoded.append(escaped)
            index += 1
            continue
        escape_map = {
            ord("n"): b"\n",
            ord("r"): b"\r",
            ord("t"): b"\t",
            ord("b"): b"\b",
            ord("f"): b"\f",
        }
        mapped = escape_map.get(escaped)
        if mapped is not None:
            decoded.extend(mapped)
            index += 1
            continue
        if 48 <= escaped <= 55:
            octal_digits = bytes([escaped])
            index += 1
            for _ in range(2):
                if index < len(raw) and 48 <= raw[index] <= 55:
                    octal_digits += bytes([raw[index]])
                    index += 1
                else:
                    break
            decoded.append(int(octal_digits, 8))
            continue
        decoded.append(escaped)
        index += 1
    return decoded.decode("utf-8", errors="ignore")


def _capture_url_images(
    workspace: Workspace,
    page_url: str,
    slug: str,
    images: List[HtmlImageReference],
    force: bool,
) -> List[CapturedImage]:
    if not images:
        return []
    asset_dir = workspace.raw_dir / "urls" / f"{slug}-assets"
    if asset_dir.exists() and force:
        shutil.rmtree(asset_dir)
    asset_dir.mkdir(parents=True, exist_ok=True)

    captured: List[CapturedImage] = []
    for index, image in enumerate(images, start=1):
        try:
            payload, extension = _read_url_image_bytes(image.source, page_url)
        except IngestError:
            continue
        base_name = slugify(image.alt_text or "image") or "image"
        filename = f"{base_name}-{index}{extension}"
        file_path = asset_dir / filename
        file_path.write_bytes(payload)
        captured.append(
            CapturedImage(
                source=image.source,
                local_markdown_path=f"{asset_dir.name}/{filename}",
                filename=filename,
                alt_text=image.alt_text,
            )
        )
    return captured


def _read_url_image_bytes(source: str, page_url: str) -> Tuple[bytes, str]:
    if source.startswith("data:"):
        return _decode_data_url(source)
    resolved = urljoin(page_url, source)
    try:
        with urlopen(resolved) as response:  # nosec B310 - intentional CLI fetch helper
            payload = response.read()
            content_type = response.headers.get_content_type() if response.headers else ""
    except Exception as error:
        raise IngestError(f"Could not fetch image: {resolved}") from error
    return payload, _extension_from_content_type(content_type, resolved)


def _decode_data_url(source: str) -> Tuple[bytes, str]:
    header, _, data = source.partition(",")
    if not data:
        raise IngestError("Malformed data URL image.")
    metadata = header[5:] if header.startswith("data:") else ""
    content_type = metadata.split(";", 1)[0] or "application/octet-stream"
    if ";base64" in metadata:
        try:
            payload = base64.b64decode(data)
        except binascii.Error as error:
            raise IngestError("Malformed base64 image payload.") from error
    else:
        payload = unquote_to_bytes(data)
    return payload, _extension_from_content_type(content_type, "")


def _extension_from_content_type(content_type: str, url: str) -> str:
    guessed = mimetypes.guess_extension(content_type or "")
    if guessed:
        return guessed
    suffix = Path(urlparse(url).path).suffix
    if suffix:
        return suffix
    return ".bin"


def _render_repository_tree(source_dir: Path, max_entries: int = 80) -> List[str]:
    lines: List[str] = []
    emitted = 0

    def visit(path: Path, depth: int) -> bool:
        nonlocal emitted
        entries = sorted(
            [entry for entry in path.iterdir() if entry.name != ".git"],
            key=lambda entry: (entry.is_file(), entry.name.lower()),
        )
        for entry in entries:
            if emitted >= max_entries:
                lines.append("... truncated ...")
                return False
            indent = "  " * depth
            label = f"{indent}{entry.name}/" if entry.is_dir() else f"{indent}{entry.name}"
            lines.append(label)
            emitted += 1
            if entry.is_dir() and not visit(entry, depth + 1):
                return False
        return True

    visit(source_dir, 0)
    return lines


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "data":
        return "data-url-capture"
    tail = parsed.path.rsplit("/", 1)[-1]
    if tail:
        return tail
    host = parsed.netloc or "url-capture"
    return host.replace(":", "-")


def _prepare_repo_checkout(repo_source) -> Tuple[Path, Optional[tempfile.TemporaryDirectory], Optional[str]]:
    source_value = str(repo_source)
    candidate_path = Path(source_value).expanduser()
    if candidate_path.exists() and candidate_path.is_dir():
        return candidate_path.resolve(), None, None

    parsed = urlparse(source_value)
    looks_remote = bool(parsed.scheme) or "://" in source_value or source_value.endswith(".git")
    if not looks_remote:
        raise IngestError(f"Repository path does not exist: {candidate_path.resolve()}")

    temp_dir = tempfile.TemporaryDirectory(prefix="cognisync-repo-")
    checkout_path = Path(temp_dir.name) / "repo"
    result = subprocess.run(
        ["git", "clone", "--depth", "1", source_value, str(checkout_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        temp_dir.cleanup()
        stderr = result.stderr.strip() or result.stdout.strip() or "unknown git clone failure"
        raise IngestError(f"Could not clone repository source '{source_value}': {stderr}")
    return checkout_path, temp_dir, source_value


def _repo_name_from_source(repo_source, source_dir: Path) -> str:
    source_value = str(repo_source)
    parsed = urlparse(source_value)
    tail = parsed.path.rsplit("/", 1)[-1] if parsed.path else ""
    if tail:
        if tail.endswith(".git"):
            tail = tail[:-4]
        if tail:
            return tail
    return source_dir.name


def _load_url_list_entries(path: Path) -> List[UrlListEntry]:
    source_path = Path(path).resolve()
    if not source_path.is_file():
        raise IngestError(f"URL list does not exist: {source_path}")

    text = source_path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None

    if payload is not None:
        items = payload.get("items", payload.get("urls", [])) if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            raise IngestError("URL list JSON must be a list or an object with an `items` or `urls` list.")
        entries: List[UrlListEntry] = []
        for index, item in enumerate(items, start=1):
            if isinstance(item, str):
                entries.append(UrlListEntry(url=item))
                continue
            if not isinstance(item, dict):
                raise IngestError(f"URL list entry {index} must be a string or object.")
            url = str(item.get("url") or item.get("source") or "").strip()
            name = str(item.get("name")).strip() if item.get("name") else None
            if not url:
                raise IngestError(f"URL list entry {index} is missing `url`.")
            entries.append(UrlListEntry(url=url, name=name))
        return entries

    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines:
        raise IngestError(f"URL list is empty: {source_path}")
    return [UrlListEntry(url=line) for line in lines]


def _load_sitemap_urls(source: str) -> List[str]:
    text = _read_text_source(source)
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError as error:
        raise IngestError(f"Sitemap is not valid XML: {source}") from error

    urls = [element.text.strip() for element in root.findall(".//{*}loc") if element.text and element.text.strip()]
    urls = _dedupe_preserve_order(urls)
    if not urls:
        raise IngestError(f"No URLs found in sitemap: {source}")
    return urls


def _read_text_source(source: str) -> str:
    candidate_path = Path(source).expanduser()
    if candidate_path.exists() and candidate_path.is_file():
        return candidate_path.read_text(encoding="utf-8")

    try:
        with urlopen(source) as response:  # nosec B310 - intentional CLI fetch helper
            charset = response.headers.get_content_charset() if response.headers else "utf-8"
            return response.read().decode(charset or "utf-8", errors="ignore")
    except Exception as error:
        raise IngestError(f"Could not read text source: {source}") from error


def _load_batch_manifest(path: Path) -> List[BatchIngestEntry]:
    manifest_path = path.resolve()
    if not manifest_path.is_file():
        raise IngestError(f"Batch manifest does not exist: {manifest_path}")

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise IngestError(f"Batch manifest is not valid JSON: {manifest_path}") from error

    items = payload.get("items", payload) if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise IngestError("Batch manifest must be a list or an object with an `items` list.")

    entries: List[BatchIngestEntry] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise IngestError(f"Batch manifest entry {index} must be an object.")
        kind = str(item.get("kind", "")).strip().lower()
        source = str(item.get("source") or item.get("path") or item.get("url") or "").strip()
        name_value = item.get("name")
        name = str(name_value).strip() if name_value is not None and str(name_value).strip() else None
        if not kind or not source:
            raise IngestError(f"Batch manifest entry {index} must include `kind` and `source`.")
        entries.append(BatchIngestEntry(kind=kind, source=source, name=name))
    return entries


def _find_readme(root: Path) -> Optional[Path]:
    for candidate in ["README.md", "README.rst", "README.txt"]:
        path = root / candidate
        if path.exists():
            return path
    return None


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _language_counts(files: List[Path]) -> List[Tuple[str, int]]:
    language_map = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".tsx": "tsx",
        ".jsx": "jsx",
        ".md": "markdown",
        ".rst": "restructuredtext",
        ".json": "json",
        ".toml": "toml",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".sh": "shell",
        ".rb": "ruby",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".kt": "kotlin",
        ".swift": "swift",
        ".c": "c",
        ".cc": "cpp",
        ".cpp": "cpp",
        ".h": "c-header",
        ".hpp": "cpp-header",
    }
    counts: Dict[str, int] = {}
    for path in files:
        language = language_map.get(path.suffix.lower())
        if language is None:
            continue
        counts[language] = counts.get(language, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _git_output_lines(cwd: Path, command: List[str]) -> List[str]:
    output = _git_output(cwd, command)
    if not output:
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def _git_output(cwd: Path, command: List[str]) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()
