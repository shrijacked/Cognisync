from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import subprocess
from typing import Dict, List, Optional

from cognisync.config import CognisyncConfig


class AdapterError(RuntimeError):
    pass


@dataclass
class CommandAdapter:
    name: str
    command: List[str]
    working_directory: str = "."
    environment: Dict[str, str] = field(default_factory=dict)

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
        cwd = Path(self.working_directory)
        if not cwd.is_absolute():
            cwd = workspace_root / cwd
        command = self.render_command(prompt_file=prompt_file, workspace_root=workspace_root, output_file=output_file)
        env = os.environ.copy()
        env.update(self.environment)
        return subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            check=check,
        )


def adapter_from_config(config: CognisyncConfig, profile_name: str) -> CommandAdapter:
    profile = config.llm_profiles.get(profile_name)
    if profile is None:
        raise AdapterError(f"LLM profile '{profile_name}' is not configured.")
    if not profile.command:
        raise AdapterError(f"LLM profile '{profile_name}' has no command configured.")
    return CommandAdapter(
        name=profile_name,
        command=profile.command,
        working_directory=profile.working_directory,
        environment=profile.environment,
    )
