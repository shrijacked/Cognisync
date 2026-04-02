from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Dict, List


@dataclass
class LLMProfile:
    command: List[str]
    working_directory: str = "."
    environment: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "LLMProfile":
        return cls(
            command=list(data.get("command", [])),
            working_directory=str(data.get("working_directory", ".")),
            environment={str(key): str(value) for key, value in dict(data.get("environment", {})).items()},
        )


@dataclass
class CognisyncConfig:
    schema_version: int = 1
    workspace_name: str = "Cognisync Workspace"
    summary_directory: str = "wiki/sources"
    concept_directory: str = "wiki/concepts"
    query_directory: str = "wiki/queries"
    report_directory: str = "outputs/reports"
    slide_directory: str = "outputs/slides"
    llm_profiles: Dict[str, LLMProfile] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["llm_profiles"] = {name: profile.to_dict() for name, profile in self.llm_profiles.items()}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "CognisyncConfig":
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            workspace_name=str(data.get("workspace_name", "Cognisync Workspace")),
            summary_directory=str(data.get("summary_directory", "wiki/sources")),
            concept_directory=str(data.get("concept_directory", "wiki/concepts")),
            query_directory=str(data.get("query_directory", "wiki/queries")),
            report_directory=str(data.get("report_directory", "outputs/reports")),
            slide_directory=str(data.get("slide_directory", "outputs/slides")),
            llm_profiles={
                str(name): LLMProfile.from_dict(profile)
                for name, profile in dict(data.get("llm_profiles", {})).items()
            },
        )


def default_config(name: str = "Cognisync Workspace") -> CognisyncConfig:
    return CognisyncConfig(workspace_name=name)


def load_config(path: Path) -> CognisyncConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    return CognisyncConfig.from_dict(data)


def save_config(path: Path, config: CognisyncConfig) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
