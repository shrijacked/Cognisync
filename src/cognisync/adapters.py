from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import subprocess
from typing import Dict, List, Optional

from cognisync.config import CognisyncConfig, LLMProfile


class AdapterError(RuntimeError):
    pass


VALID_STDIN_SOURCES = {"none", "prompt_file"}


@dataclass
class CommandAdapter:
    name: str
    command: List[str]
    working_directory: str = "."
    environment: Dict[str, str] = field(default_factory=dict)
    stdin_source: str = "none"
    output_file_flag: Optional[str] = None
    description: str = ""

    def render_command(self, prompt_file: Path, workspace_root: Path, output_file: Optional[Path] = None) -> List[str]:
        mapping = {
            "prompt_file": str(prompt_file),
            "workspace_root": str(workspace_root),
            "output_file": str(output_file) if output_file else "",
        }
        return [part.format(**mapping) for part in self.command]

    def run(
        self,
        prompt_file: Path,
        workspace_root: Path,
        output_file: Optional[Path] = None,
        check: bool = False,
    ) -> subprocess.CompletedProcess:
        if self.stdin_source not in VALID_STDIN_SOURCES:
            raise AdapterError(
                f"Adapter '{self.name}' uses unsupported stdin_source '{self.stdin_source}'. "
                f"Expected one of {sorted(VALID_STDIN_SOURCES)}."
            )
        cwd = Path(self.working_directory)
        if not cwd.is_absolute():
            cwd = workspace_root / cwd
        command = self.render_command(prompt_file=prompt_file, workspace_root=workspace_root, output_file=output_file)
        if output_file and self.output_file_flag:
            command.extend([self.output_file_flag, str(output_file)])
        env = os.environ.copy()
        env.update(self.environment)
        stdin_text = None
        if self.stdin_source == "prompt_file":
            stdin_text = prompt_file.read_text(encoding="utf-8")
        return subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            input=stdin_text,
            check=check,
        )


@dataclass(frozen=True)
class BuiltinAdapterPreset:
    name: str
    summary: str
    profile: LLMProfile


def builtin_adapter_presets() -> Dict[str, BuiltinAdapterPreset]:
    return {
        "codex": BuiltinAdapterPreset(
            name="codex",
            summary="OpenAI Codex CLI profile for non-interactive prompt-packet execution.",
            profile=LLMProfile(
                command=[
                    "codex",
                    "exec",
                    "--skip-git-repo-check",
                    "--sandbox",
                    "workspace-write",
                    "--full-auto",
                    "--cd",
                    "{workspace_root}",
                ],
                stdin_source="prompt_file",
                output_file_flag="--output-last-message",
                description=(
                    "Runs prompt packets through `codex exec`, streams the packet over stdin, "
                    "and optionally writes the last message to a requested output file."
                ),
            ),
        ),
        "gemini": BuiltinAdapterPreset(
            name="gemini",
            summary="Google Gemini CLI profile for non-interactive prompt-packet execution.",
            profile=LLMProfile(
                command=[
                    "gemini",
                    "--prompt",
                    "Follow the instructions provided on stdin and return the final answer on stdout.",
                    "--yolo",
                ],
                stdin_source="prompt_file",
                description=(
                    "Runs prompt packets through Gemini CLI in non-interactive mode by streaming the "
                    "packet over stdin and capturing the model response from stdout."
                ),
            ),
        ),
    }


def install_builtin_adapter(
    config: CognisyncConfig,
    preset_name: str,
    profile_name: Optional[str] = None,
    force: bool = False,
) -> LLMProfile:
    presets = builtin_adapter_presets()
    preset = presets.get(preset_name)
    if preset is None:
        raise AdapterError(
            f"Unknown builtin adapter '{preset_name}'. Available presets: {', '.join(sorted(presets))}."
        )
    target_name = profile_name or preset_name
    if target_name in config.llm_profiles and not force:
        raise AdapterError(
            f"LLM profile '{target_name}' already exists. Re-run with --force to overwrite it."
        )

    profile = LLMProfile.from_dict(preset.profile.to_dict())
    config.llm_profiles[target_name] = profile
    return profile


def adapter_from_config(config: CognisyncConfig, profile_name: str) -> CommandAdapter:
    profile = config.llm_profiles.get(profile_name)
    if profile is None:
        raise AdapterError(f"LLM profile '{profile_name}' is not configured.")
    if not profile.command:
        raise AdapterError(f"LLM profile '{profile_name}' has no command configured.")
    if profile.stdin_source not in VALID_STDIN_SOURCES:
        raise AdapterError(
            f"LLM profile '{profile_name}' has unsupported stdin_source '{profile.stdin_source}'. "
            f"Expected one of {sorted(VALID_STDIN_SOURCES)}."
        )
    return CommandAdapter(
        name=profile_name,
        command=profile.command,
        working_directory=profile.working_directory,
        environment=profile.environment,
        stdin_source=profile.stdin_source,
        output_file_flag=profile.output_file_flag,
        description=profile.description,
    )
