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
class ResearchPlanStep:
    step_id: str
    kind: str
    title: str
    status: str
    detail: str
    owner: str = "operator"
    output_path: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ResearchPlanStep":
        return cls(
            step_id=str(data["step_id"]),
            kind=str(data["kind"]),
            title=str(data["title"]),
            status=str(data["status"]),
            detail=str(data.get("detail", "")),
            owner=str(data.get("owner", "operator")),
            output_path=str(data["output_path"]) if data.get("output_path") is not None else None,
            depends_on=list(data.get("depends_on", [])),
        )


@dataclass
class ResearchPlan:
    generated_at: str
    question: str
    mode: str
    job_profile: str
    report_path: str
    packet_path: str
    source_packet_path: Optional[str]
    answer_path: str
    slide_path: Optional[str]
    notes_dir: Optional[str]
    note_paths: List[str]
    checkpoints_path: Optional[str]
    validation_report_path: Optional[str]
    sources: List[Dict[str, object]]
    steps: List[ResearchPlanStep]

    def to_dict(self) -> Dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "question": self.question,
            "mode": self.mode,
            "job_profile": self.job_profile,
            "report_path": self.report_path,
            "packet_path": self.packet_path,
            "source_packet_path": self.source_packet_path,
            "answer_path": self.answer_path,
            "slide_path": self.slide_path,
            "notes_dir": self.notes_dir,
            "note_paths": self.note_paths,
            "checkpoints_path": self.checkpoints_path,
            "validation_report_path": self.validation_report_path,
            "sources": self.sources,
            "steps": [step.to_dict() for step in self.steps],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ResearchPlan":
        return cls(
            generated_at=str(data["generated_at"]),
            question=str(data["question"]),
            mode=str(data["mode"]),
            job_profile=str(data.get("job_profile", "synthesis-report")),
            report_path=str(data["report_path"]),
            packet_path=str(data["packet_path"]),
            source_packet_path=str(data["source_packet_path"]) if data.get("source_packet_path") is not None else None,
            answer_path=str(data["answer_path"]),
            slide_path=str(data["slide_path"]) if data.get("slide_path") is not None else None,
            notes_dir=str(data["notes_dir"]) if data.get("notes_dir") is not None else None,
            note_paths=list(data.get("note_paths", [])),
            checkpoints_path=str(data["checkpoints_path"]) if data.get("checkpoints_path") is not None else None,
            validation_report_path=(
                str(data["validation_report_path"]) if data.get("validation_report_path") is not None else None
            ),
            sources=list(data.get("sources", [])),
            steps=[ResearchPlanStep.from_dict(item) for item in data.get("steps", [])],
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
