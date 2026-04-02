from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from cognisync.config import default_config, load_config, save_config
from cognisync.types import CompilePlan, IndexSnapshot


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def wiki_dir(self) -> Path:
        return self.root / "wiki"

    @property
    def outputs_dir(self) -> Path:
        return self.root / "outputs"

    @property
    def prompts_dir(self) -> Path:
        return self.root / "prompts"

    @property
    def state_dir(self) -> Path:
        return self.root / ".cognisync"

    @property
    def config_path(self) -> Path:
        return self.state_dir / "config.json"

    @property
    def index_path(self) -> Path:
        return self.state_dir / "index.json"

    @property
    def plans_dir(self) -> Path:
        return self.state_dir / "plans"

    @property
    def runs_dir(self) -> Path:
        return self.state_dir / "runs"

    @property
    def sources_manifest_path(self) -> Path:
        return self.state_dir / "sources.json"

    @property
    def graph_manifest_path(self) -> Path:
        return self.state_dir / "graph.json"

    def initialize(self, name: Optional[str] = None, force: bool = False) -> None:
        directories = [
            self.raw_dir,
            self.wiki_dir / "sources",
            self.wiki_dir / "concepts",
            self.wiki_dir / "queries",
            self.outputs_dir / "reports",
            self.outputs_dir / "slides",
            self.prompts_dir,
            self.state_dir,
            self.plans_dir,
            self.runs_dir,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        if force or not self.config_path.exists():
            save_config(self.config_path, default_config(name or self.root.name))

        wiki_index = self.wiki_dir / "index.md"
        if force or not wiki_index.exists():
            wiki_index.write_text(
                "# Knowledge Base Index\n\n"
                "This workspace is managed by Cognisync.\n\n"
                "## Sections\n\n"
                "- [[sources]]\n"
                "- [[concepts]]\n"
                "- [[queries]]\n",
                encoding="utf-8",
            )

        navigation_pages = {
            self.wiki_dir / "sources.md": (
                "# Sources\n\n"
                "Use this page to index or curate compiled source summaries stored in `wiki/sources/`.\n"
            ),
            self.wiki_dir / "concepts.md": (
                "# Concepts\n\n"
                "Use this page to organize shared concepts synthesized from multiple sources.\n"
            ),
            self.wiki_dir / "queries.md": (
                "# Queries\n\n"
                "Use this page to catalogue question-driven investigations and reusable answers.\n"
            ),
        }
        for path, content in navigation_pages.items():
            if force or not path.exists():
                path.write_text(content, encoding="utf-8")

    def load_config(self):
        return load_config(self.config_path)

    def relative_path(self, path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(self.root).as_posix()
        except ValueError:
            return resolved.as_posix()

    def write_index(self, snapshot: IndexSnapshot) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(snapshot.to_dict(), indent=2, sort_keys=True), encoding="utf-8")

    def read_index(self) -> IndexSnapshot:
        data = json.loads(self.index_path.read_text(encoding="utf-8"))
        return IndexSnapshot.from_dict(data)

    def write_plan_json(self, name: str, plan: CompilePlan) -> Path:
        path = self.plans_dir / f"{name}.json"
        path.write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path
