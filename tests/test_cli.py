import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config


class CliTests(unittest.TestCase):
    def test_demo_command_creates_a_populated_example_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "demo-workspace"

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["demo", str(root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((root / "README.md").exists())
            self.assertTrue((root / "raw" / "agentic-workflows.md").exists())
            self.assertTrue((root / "wiki" / "sources" / "agentic-workflows.md").exists())
            self.assertTrue((root / "wiki" / "concepts" / "knowledge-gardens.md").exists())
            self.assertTrue((root / "outputs" / "reports" / "how-should-a-team-build-an-llm-maintained-research-garden.md").exists())
            self.assertTrue((root / "outputs" / "slides" / "how-should-a-team-build-an-llm-maintained-research-garden.md").exists())
            self.assertTrue((root / "prompts" / "compile-plan.md").exists())
            self.assertTrue((root / "prompts" / "query-how-should-a-team-build-an-llm-maintained-research-garden.md").exists())
            self.assertIn("Demo workspace ready", stdout.getvalue())
            self.assertEqual(main(["doctor", "--workspace", str(root), "--strict"]), 0)
            self.assertEqual(main(["lint", "--workspace", str(root), "--strict"]), 0)

    def test_demo_command_refuses_to_overwrite_non_empty_directory_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "demo-workspace"
            root.mkdir(parents=True, exist_ok=True)
            (root / "keep.txt").write_text("keep me", encoding="utf-8")

            exit_code = main(["demo", str(root)])

            self.assertEqual(exit_code, 2)
            self.assertEqual((root / "keep.txt").read_text(encoding="utf-8"), "keep me")

    def test_cli_flow_initializes_scans_plans_lints_and_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            init_stdout = io.StringIO()
            with redirect_stdout(init_stdout):
                exit_code = main(["init", str(root), "--name", "CLI Workspace"])
            self.assertEqual(exit_code, 0)

            (root / "raw" / "doc.md").write_text(
                "# CLI Doc\n\nThis document covers orchestration workflows.\n",
                encoding="utf-8",
            )
            (root / "wiki" / "sources" / "doc.md").write_text(
                "# CLI Doc\n\nSummary page exists.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            self.assertEqual(main(["plan", "--workspace", str(root)]), 0)
            self.assertEqual(main(["lint", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(["query", "--workspace", str(root), "orchestration workflows"]),
                0,
            )

            report_files = list((root / "outputs" / "reports").glob("*.md"))
            self.assertTrue(report_files)

    def test_research_command_runs_search_packet_and_answer_filing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Research Workspace"]), 0)

            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            config = load_config(root / ".cognisync" / "config.json")
            config.llm_profiles["researcher"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Filed Answer\\n\\nAgent loops rely on structured memory. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(root / ".cognisync" / "config.json", config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "researcher",
                        "how do agent loops use memory",
                    ]
                )

            self.assertEqual(exit_code, 0)
            report_path = root / "outputs" / "reports" / "how-do-agent-loops-use-memory.md"
            answer_path = root / "wiki" / "queries" / "how-do-agent-loops-use-memory.md"
            packet_path = root / "prompts" / "query-how-do-agent-loops-use-memory.md"
            self.assertTrue(report_path.exists())
            self.assertTrue(answer_path.exists())
            self.assertTrue(packet_path.exists())
            self.assertIn("Filed Answer", answer_path.read_text(encoding="utf-8"))
            self.assertIn("Wrote filed answer", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
