import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config
from cognisync.workspace import Workspace


class AdapterTests(unittest.TestCase):
    def test_adapter_list_includes_codex(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["adapter", "list"])

        self.assertEqual(exit_code, 0)
        self.assertIn("codex:", stdout.getvalue())

    def test_run_packet_executes_configured_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Adapter Test")

            config = load_config(workspace.config_path)
            config.llm_profiles["echo"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import pathlib; print(pathlib.Path(r'{prompt_file}').read_text(encoding='utf-8').strip())",
                ]
            )
            save_config(workspace.config_path, config)

            prompt_path = workspace.prompts_dir / "packet.md"
            prompt_path.write_text("hello from packet", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-packet",
                        str(prompt_path),
                        "--workspace",
                        str(root),
                        "--profile",
                        "echo",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("hello from packet", stdout.getvalue())

    def test_run_packet_can_stream_prompt_file_to_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Stdin Adapter Test")

            config = load_config(workspace.config_path)
            config.llm_profiles["stdin-echo"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; print(sys.stdin.read().strip())",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            prompt_path = workspace.prompts_dir / "stdin-packet.md"
            prompt_path.write_text("hello from stdin packet", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-packet",
                        str(prompt_path),
                        "--workspace",
                        str(root),
                        "--profile",
                        "stdin-echo",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("hello from stdin packet", stdout.getvalue())

    def test_adapter_install_writes_builtin_codex_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Builtin Adapter Test")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "adapter",
                        "install",
                        "codex",
                        "--workspace",
                        str(root),
                        "--profile",
                        "codex",
                    ]
                )

            self.assertEqual(exit_code, 0)
            config = load_config(workspace.config_path)
            self.assertIn("codex", config.llm_profiles)

            profile = config.llm_profiles["codex"]
            self.assertEqual(profile.stdin_source, "prompt_file")
            self.assertEqual(profile.command[0], "codex")
            self.assertIn("exec", profile.command)
            self.assertEqual(profile.output_file_flag, "--output-last-message")
            self.assertIn("Installed builtin adapter 'codex'", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
