from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import posixpath
import shutil
import subprocess
from typing import List, Optional, Tuple
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


class _HtmlToMarkdownParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self._current_tag: Optional[str] = None
        self._buffer: List[str] = []
        self.blocks: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
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
    title, body = _convert_remote_text_to_markdown(text=text, content_type=content_type)
    slug = slugify(name or title or _slug_from_url(url))
    target_path = target_dir / f"{slug}.md"
    if target_path.exists() and not force:
        raise IngestError(f"Target already exists: {target_path}. Re-run with --force to overwrite it.")

    target_path.write_text(
        "\n".join(
            [
                "---",
                f"title: {title or slug}",
                "tags: [url-ingest]",
                "---",
                f"# {title or slug}",
                "",
                f"Source URL: {url}",
                f"Fetched: {datetime.now(timezone.utc).replace(microsecond=0).isoformat()}",
                "",
                body.strip(),
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

    top_level = sorted(path.name for path in source_dir.iterdir() if path.name != ".git")
    readme_path = _find_readme(source_dir)
    readme_excerpt = ""
    if readme_path:
        readme_excerpt = "\n".join(readme_path.read_text(encoding="utf-8", errors="ignore").splitlines()[:12]).strip()

    lines = [
        f"# {repo_name}",
        "",
        f"Source path: `{source_dir}`",
        "",
    ]
    if branch:
        lines.append(f"Current branch: `{branch}`")
    if commit:
        lines.append(f"Current commit: `{commit}`")
    if remote:
        lines.append(f"Origin remote: `{remote}`")
    lines.extend(["", "## Top-level tree", ""])
    for entry in top_level[:30]:
        lines.append(f"- `{entry}`")
    if readme_excerpt:
        lines.extend(["", "## README excerpt", "", readme_excerpt, ""])

    target_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return IngestResult(path=target_path, kind="repo")


def _convert_remote_text_to_markdown(text: str, content_type: str) -> Tuple[str, str]:
    if "html" in content_type:
        parser = _HtmlToMarkdownParser()
        parser.feed(text)
        body = "\n\n".join(block for block in parser.blocks if block)
        title = parser.title or "Web Capture"
        return title, body or text
    if "json" in content_type:
        parsed = json.loads(text)
        return "JSON Capture", "```json\n" + json.dumps(parsed, indent=2, sort_keys=True) + "\n```"
    return "Web Capture", text


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
