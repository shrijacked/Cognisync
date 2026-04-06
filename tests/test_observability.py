import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class ObservabilityTests(unittest.TestCase):
    def test_audit_and_usage_commands_materialize_control_plane_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Observability Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Databases\n\nVector Databases improve recall.\n",
                encoding="utf-8",
            )
            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "reviewer-1",
                        "reviewer",
                        "--workspace",
                        str(root),
                        "--name",
                        "Reviewer One",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://control.example.test/api",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "invite-peer",
                        "remote-ops",
                        "operator",
                        "--workspace",
                        str(root),
                        "--base-url",
                        "https://remote.example.test",
                        "--capability",
                        "control.read",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--scope",
                        "control.read",
                        "--output-file",
                        str(root / "outputs" / "reports" / "exports" / "token.json"),
                    ]
                ),
                0,
            )
            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        "data:text/html;charset=utf-8,<html><head><title>Connector Page</title></head><body><p>Connector body.</p></body></html>",
                        "--workspace",
                        str(root),
                        "--name",
                        "connector-page",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(main(["sync", "export", "--workspace", str(root)]), 0)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                audit_exit = main(["audit", "list", "--workspace", str(root)])
            self.assertEqual(audit_exit, 0)
            self.assertIn("Audit History", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                usage_exit = main(["usage", "report", "--workspace", str(root)])
            self.assertEqual(usage_exit, 0)
            self.assertIn("Usage Summary", stdout.getvalue())

            audit_path = root / ".cognisync" / "audit.json"
            usage_path = root / ".cognisync" / "usage.json"
            self.assertTrue(audit_path.exists())
            self.assertTrue(usage_path.exists())

            audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
            usage_payload = json.loads(usage_path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(audit_payload["summary"]["total_count"], 2)
            event_kinds = {item["event_kind"] for item in audit_payload["events"]}
            self.assertIn("job", event_kinds)
            self.assertIn("sync", event_kinds)
            self.assertIn("sharing", event_kinds)
            self.assertIn("control_plane", event_kinds)

            summary = usage_payload["summary"]
            self.assertGreaterEqual(summary["access_member_count"], 2)
            self.assertGreaterEqual(summary["connector_count"], 1)
            self.assertGreaterEqual(summary["job_count"], 1)
            self.assertGreaterEqual(summary["sync_event_count"], 1)
            self.assertGreaterEqual(summary["shared_peer_count"], 1)
            self.assertGreaterEqual(summary["accepted_shared_peer_count"], 1)
            self.assertGreaterEqual(summary["active_control_plane_token_count"], 1)
            self.assertIn("reviewer", summary["access_counts_by_role"])


if __name__ == "__main__":
    unittest.main()
