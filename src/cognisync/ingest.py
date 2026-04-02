from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import shutil
import subprocess
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import urlopen

from cognisync.utils import slugify
from cognisync.workspace import Workspace


class IngestError(RuntimeError):
    pass


@dataclass(frozen=True)
class IngestResult:
    path: Path
    kind: str


@dataclass(frozen=True)
class HtmlCapture:
    title: str
    description: str
    canonical_url: str
    body_markdown: str
    headings: List[str]
    links: List[str]
    word_count: int


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
    return ingest_file(workspace, source=source, category="pdfs", name=name, force=force)


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


def ingest_repo(workspace: Workspace, repo_path: Path, name: Optional[str] = None, force: bool = False) -> IngestResult:
    source_dir = Path(repo_path).resolve()
    if not source_dir.is_dir():
        raise IngestError(f"Repository path does not exist: {source_dir}")

    repo_name = name or source_dir.name
    target_dir = workspace.raw_dir / "repos"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{slugify(repo_name)}.md"
    if target_path.exists() and not force:
        raise IngestError(f"Target already exists: {target_path}. Re-run with --force to overwrite it.")

    branch = _git_output(source_dir, ["git", "branch", "--show-current"])
    commit = _git_output(source_dir, ["git", "rev-parse", "--short", "HEAD"])
    remote = _git_output(source_dir, ["git", "remote", "get-url", "origin"])
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
    if readme_excerpt:
        lines.extend(["", "## README excerpt", "", readme_excerpt, ""])

    target_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return IngestResult(path=target_path, kind="repo")


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
            word_count=len(body.split()),
        )
    return HtmlCapture(
        title="Web Capture",
        description="",
        canonical_url="",
        body_markdown=text,
        headings=[],
        links=[],
        word_count=len(text.split()),
    )


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "data":
        return "data-url-capture"
    tail = parsed.path.rsplit("/", 1)[-1]
    if tail:
        return tail
    host = parsed.netloc or "url-capture"
    return host.replace(":", "-")


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
