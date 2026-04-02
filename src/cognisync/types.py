from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass
class LinkReference:
    raw_target: str
    resolved_path: Optional[str]
    external: bool = False
    kind: str = "link"

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "LinkReference":
        return cls(
            raw_target=str(data.get("raw_target", "")),
            resolved_path=data.get("resolved_path"),
            external=bool(data.get("external", False)),
            kind=str(data.get("kind", "link")),
        )


@dataclass
class ArtifactRecord:
    path: str
    collection: str
    kind: str
    title: str
    word_count: int
    headings: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    links: List[LinkReference] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    summary_target: Optional[str] = None
    content_hash: str = ""
    modified_at: float = 0.0

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["links"] = [link.to_dict() for link in self.links]
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ArtifactRecord":
        return cls(
            path=str(data["path"]),
            collection=str(data["collection"]),
            kind=str(data["kind"]),
            title=str(data["title"]),
            word_count=int(data.get("word_count", 0)),
            headings=list(data.get("headings", [])),
            tags=list(data.get("tags", [])),
            links=[LinkReference.from_dict(item) for item in data.get("links", [])],
            images=list(data.get("images", [])),
            summary_target=data.get("summary_target"),
            content_hash=str(data.get("content_hash", "")),
            modified_at=float(data.get("modified_at", 0.0)),
        )


@dataclass
class IndexSnapshot:
    generated_at: str
    artifacts: List[ArtifactRecord]
    backlinks: Dict[str, List[str]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "backlinks": self.backlinks,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "IndexSnapshot":
        return cls(
            generated_at=str(data["generated_at"]),
            artifacts=[ArtifactRecord.from_dict(item) for item in data.get("artifacts", [])],
            backlinks={str(key): list(value) for key, value in dict(data.get("backlinks", {})).items()},
        )

    def artifact_by_path(self, path: str) -> ArtifactRecord:
        for artifact in self.artifacts:
            if artifact.path == path:
                return artifact
        raise KeyError(path)

    def artifact_paths(self) -> List[str]:
        return [artifact.path for artifact in self.artifacts]

    def markdown_artifacts(self) -> List[ArtifactRecord]:
        return [artifact for artifact in self.artifacts if artifact.kind == "markdown"]


@dataclass
class PlanTask:
    task_id: str
    kind: str
    title: str
    inputs: List[str]
    output_path: str
    rationale: str
    prompt_hint: str

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "PlanTask":
        return cls(
            task_id=str(data["task_id"]),
            kind=str(data["kind"]),
            title=str(data["title"]),
            inputs=list(data.get("inputs", [])),
            output_path=str(data["output_path"]),
            rationale=str(data.get("rationale", "")),
            prompt_hint=str(data.get("prompt_hint", "")),
        )


@dataclass
class CompilePlan:
    generated_at: str
    tasks: List[PlanTask]

    def to_dict(self) -> Dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "tasks": [task.to_dict() for task in self.tasks],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "CompilePlan":
        return cls(
            generated_at=str(data["generated_at"]),
            tasks=[PlanTask.from_dict(item) for item in data.get("tasks", [])],
        )


@dataclass
class LintIssue:
    issue_id: str
    kind: str
    severity: str
    path: str
    message: str
    suggestion: str

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "LintIssue":
        return cls(
            issue_id=str(data["issue_id"]),
            kind=str(data["kind"]),
            severity=str(data["severity"]),
            path=str(data["path"]),
            message=str(data["message"]),
            suggestion=str(data.get("suggestion", "")),
        )


@dataclass
class SearchHit:
    path: str
    title: str
    score: float
    snippet: str
    source_kind: str = "artifact"
    retrieval_reason: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)
