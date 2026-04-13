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
    assignment_id: Optional[str] = None
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
            assignment_id=str(data["assignment_id"]) if data.get("assignment_id") is not None else None,
            output_path=str(data["output_path"]) if data.get("output_path") is not None else None,
            depends_on=list(data.get("depends_on", [])),
        )


@dataclass
class ResearchStepAssignment:
    assignment_id: str
    step_id: str
    title: str
    agent_role: str
    adapter_profile: Optional[str] = None
    worker_capability: str = "research"
    execution_mode: str = "remote_eligible"
    validation_rules: List[str] = field(default_factory=list)
    review_roles: List[str] = field(default_factory=list)
    depends_on_assignment_ids: List[str] = field(default_factory=list)
    output_path: Optional[str] = None
    status: str = "planned"

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ResearchStepAssignment":
        return cls(
            assignment_id=str(data["assignment_id"]),
            step_id=str(data["step_id"]),
            title=str(data["title"]),
            agent_role=str(data["agent_role"]),
            adapter_profile=str(data["adapter_profile"]) if data.get("adapter_profile") is not None else None,
            worker_capability=str(data.get("worker_capability", "research")),
            execution_mode=str(data.get("execution_mode", "remote_eligible")),
            validation_rules=list(data.get("validation_rules", [])),
            review_roles=list(data.get("review_roles", [])),
            depends_on_assignment_ids=list(data.get("depends_on_assignment_ids", [])),
            output_path=str(data["output_path"]) if data.get("output_path") is not None else None,
            status=str(data.get("status", "planned")),
        )


@dataclass
class ResearchAgentPlan:
    generated_at: str
    question: str
    job_profile: str
    run_manifest_path: str
    checkpoints_path: str
    default_profile: Optional[str]
    assignments: List[ResearchStepAssignment]
    summary_counts: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "question": self.question,
            "job_profile": self.job_profile,
            "run_manifest_path": self.run_manifest_path,
            "checkpoints_path": self.checkpoints_path,
            "default_profile": self.default_profile,
            "assignments": [assignment.to_dict() for assignment in self.assignments],
            "summary_counts": dict(self.summary_counts),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "ResearchAgentPlan":
        return cls(
            generated_at=str(data["generated_at"]),
            question=str(data["question"]),
            job_profile=str(data.get("job_profile", "synthesis-report")),
            run_manifest_path=str(data["run_manifest_path"]),
            checkpoints_path=str(data["checkpoints_path"]),
            default_profile=str(data["default_profile"]) if data.get("default_profile") is not None else None,
            assignments=[ResearchStepAssignment.from_dict(item) for item in data.get("assignments", [])],
            summary_counts={str(key): int(value) for key, value in dict(data.get("summary_counts", {})).items()},
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
    agent_plan_path: Optional[str]
    sources: List[Dict[str, object]]
    steps: List[ResearchPlanStep]
    assignments: List[ResearchStepAssignment] = field(default_factory=list)

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
            "agent_plan_path": self.agent_plan_path,
            "sources": self.sources,
            "steps": [step.to_dict() for step in self.steps],
            "assignments": [assignment.to_dict() for assignment in self.assignments],
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
            agent_plan_path=str(data["agent_plan_path"]) if data.get("agent_plan_path") is not None else None,
            sources=list(data.get("sources", [])),
            steps=[ResearchPlanStep.from_dict(item) for item in data.get("steps", [])],
            assignments=[ResearchStepAssignment.from_dict(item) for item in data.get("assignments", [])],
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
