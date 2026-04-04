import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config


class NotificationTests(unittest.TestCase):
    def test_notify_list_writes_a_durable_operator_inbox(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Notification Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Databases\n\nVector Databases improve recall.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )
            (root / "raw" / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (root / "raw" / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        "data:text/html;charset=utf-8,<html><head><title>Alert Source</title></head><body><p>Connector backlog.</p></body></html>",
                        "--workspace",
                        str(root),
                        "--name",
                        "alert-source",
                    ]
                ),
                0,
            )

            config = load_config(root / ".cognisync" / "config.json")
            config.llm_profiles["failing"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops always require vector databases.')",
                ]
            )
            save_config(root / ".cognisync" / "config.json", config)

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "failing",
                        "do agent loops always require vector databases",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["jobs", "run-next", "--workspace", str(root)]), 2)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                notify_exit = main(["notify", "list", "--workspace", str(root)])
            self.assertEqual(notify_exit, 0)
            self.assertIn("Notifications", stdout.getvalue())
            self.assertIn("connector registry has", stdout.getvalue().lower())
            self.assertIn("job failed", stdout.getvalue().lower())
            self.assertIn("research validation failed", stdout.getvalue().lower())

            manifest_path = root / ".cognisync" / "notifications.json"
            self.assertTrue(manifest_path.exists())
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(payload["summary"]["total_count"], 3)
            kinds = {item["kind"] for item in payload["notifications"]}
            self.assertIn("connector_backlog", kinds)
            self.assertIn("job_failed", kinds)
            self.assertIn("run_failed_validation", kinds)


if __name__ == "__main__":
    unittest.main()
