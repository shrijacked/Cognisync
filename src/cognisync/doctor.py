from __future__ import annotations

from dataclasses import dataclass
import os
import shutil
from typing import List

from cognisync.config import CognisyncConfig
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    detail: str
    remedy: str = ""


def run_doctor(workspace: Workspace) -> List[DoctorCheck]:
    checks: List[DoctorCheck] = []

    required_dirs = [
        workspace.raw_dir,
        workspace.wiki_dir,
        workspace.outputs_dir,
        workspace.prompts_dir,
        workspace.state_dir,
    ]
    missing_dirs = [path for path in required_dirs if not path.exists()]
    if missing_dirs:
        checks.append(
            DoctorCheck(
                name="workspace_layout",
                status="fail",
                detail="Missing required workspace directories.",
                remedy="Run `cognisync init` to create the expected layout.",
            )
        )
        return checks

    config = None
    if workspace.config_path.exists():
        try:
            config = workspace.load_config()
            checks.append(
                DoctorCheck(
                    name="workspace_config",
                    status="pass",
                    detail=f"Loaded {workspace.config_path}.",
                )
            )
        except Exception as error:  # pragma: no cover - defensive guard
            checks.append(
                DoctorCheck(
                    name="workspace_config",
                    status="fail",
                    detail=f"Could not load config: {error}",
                    remedy="Repair or recreate `.cognisync/config.json`.",
                )
            )
            return checks
    else:
        checks.append(
            DoctorCheck(
                name="workspace_config",
                status="fail",
                detail="Missing `.cognisync/config.json`.",
                remedy="Run `cognisync init` to initialize the workspace.",
            )
        )
        return checks

    if workspace.index_path.exists():
        checks.append(
            DoctorCheck(
                name="index_snapshot",
                status="pass",
                detail=f"Index snapshot exists at {workspace.index_path}.",
            )
        )
    else:
        checks.append(
            DoctorCheck(
                name="index_snapshot",
                status="warn",
                detail="No index snapshot found yet.",
                remedy="Run `cognisync scan` after ingesting content.",
            )
        )

    checks.extend(_profile_checks(config))
    checks.extend(_maintenance_policy_checks(config))
    return checks


def _profile_checks(config: CognisyncConfig) -> List[DoctorCheck]:
    if not config.llm_profiles:
        return [
            DoctorCheck(
                name="llm_profiles",
                status="warn",
                detail="No LLM profiles configured.",
                remedy="Install a builtin profile with `cognisync adapter install codex`, `gemini`, or `claude`.",
            )
        ]

    checks: List[DoctorCheck] = []
    for name in sorted(config.llm_profiles):
        profile = config.llm_profiles[name]
        if not profile.command:
            checks.append(
                DoctorCheck(
                    name=f"profile:{name}",
                    status="fail",
                    detail="Profile has no command configured.",
                    remedy="Edit `.cognisync/config.json` or reinstall the profile.",
                )
            )
            continue

        executable = profile.command[0]
        available = False
        if "/" in executable or executable.startswith("."):
            available = shutil.which(executable) is not None or os.path.exists(executable)
        else:
            available = shutil.which(executable) is not None

        if available:
            checks.append(
                DoctorCheck(
                    name=f"profile:{name}",
                    status="pass",
                    detail=f"Resolved adapter command `{executable}`.",
                )
            )
        else:
            checks.append(
                DoctorCheck(
                    name=f"profile:{name}",
                    status="fail",
                    detail=f"Could not resolve adapter command `{executable}`.",
                    remedy="Install the CLI or update the configured command path.",
                )
            )
    return checks


def _maintenance_policy_checks(config: CognisyncConfig) -> List[DoctorCheck]:
    policy = config.maintenance_policy
    detail = (
        "Maintenance policy: "
        f"min_support={policy.min_concept_support}, "
        f"require_entity_for_short={str(policy.require_entity_evidence_for_short_concepts).lower()}, "
        f"deny_concepts={len(policy.deny_concepts)}."
    )
    if policy.min_concept_support <= 1 or not policy.require_entity_evidence_for_short_concepts:
        return [
            DoctorCheck(
                name="maintenance_policy",
                status="warn",
                detail=detail,
                remedy=(
                    "Raise `min_concept_support` or require entity evidence for short concepts "
                    "if maintenance is creating low-signal concept pages."
                ),
            )
        ]
    return [
        DoctorCheck(
            name="maintenance_policy",
            status="pass",
            detail=detail,
        )
    ]


def render_doctor_report(checks: List[DoctorCheck]) -> str:
    lines = ["# Cognisync Doctor", ""]
    for check in checks:
        lines.append(f"{check.status.upper()} {check.name}: {check.detail}")
        if check.remedy:
            lines.append(f"  remedy: {check.remedy}")
    return "\n".join(lines)


def doctor_exit_code(checks: List[DoctorCheck], strict: bool) -> int:
    if strict and any(check.status == "fail" for check in checks):
        return 1
    return 0
