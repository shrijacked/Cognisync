import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class CollaborationTests(unittest.TestCase):
    def test_collab_cli_tracks_requests_comments_and_decisions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            artifact_path = root / "outputs" / "reports" / "research-brief.md"
            artifact_rel = "outputs/reports/research-brief.md"

            self.assertEqual(main(["init", str(root), "--name", "Collaboration Workspace"]), 0)
            self.assertEqual(main(["access", "grant", "editor-1", "editor", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            artifact_path.write_text("# Research Brief\n\nA cited artifact.\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "collab",
                        "request-review",
                        artifact_rel,
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "editor-1",
                        "--assign",
                        "reviewer-1",
                        "--note",
                        "check the citations",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "collab",
                        "comment",
                        artifact_rel,
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                        "--message",
                        "the structure looks good",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "collab",
                        "approve",
                        artifact_rel,
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                        "--summary",
                        "approved after review",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(main(["collab", "list", "--workspace", str(root)]), 0)
            self.assertIn("research-brief.md", stdout.getvalue())
            self.assertIn("approved", stdout.getvalue().lower())

            manifest_path = root / ".cognisync" / "collaboration.json"
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["thread_count"], 1)
            self.assertEqual(payload["summary"]["counts_by_status"]["approved"], 1)
            thread = payload["threads"][0]
            self.assertEqual(thread["artifact_path"], artifact_rel)
            self.assertEqual(thread["status"], "approved")
            self.assertEqual(thread["requested_by"]["principal_id"], "editor-1")
            self.assertEqual(thread["assignees"][0]["principal_id"], "reviewer-1")
            self.assertGreaterEqual(len(thread["comments"]), 1)
            self.assertEqual(thread["decisions"][-1]["decision"], "approved")

    def test_collaboration_state_flows_into_notifications_sync_audit_and_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            artifact_path = root / "outputs" / "reports" / "report.md"
            artifact_rel = "outputs/reports/report.md"

            self.assertEqual(main(["init", str(root), "--name", "Collaboration State Workspace"]), 0)
            self.assertEqual(main(["access", "grant", "editor-1", "editor", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            artifact_path.write_text("# Report\n\nNeeds stronger grounding.\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "collab",
                        "request-review",
                        artifact_rel,
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "editor-1",
                        "--assign",
                        "reviewer-1",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "collab",
                        "request-changes",
                        artifact_rel,
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                        "--summary",
                        "add stronger grounding",
                    ]
                ),
                0,
            )

            self.assertEqual(main(["notify", "list", "--workspace", str(root)]), 0)
            notifications_payload = json.loads((root / ".cognisync" / "notifications.json").read_text(encoding="utf-8"))
            notification_kinds = {item["kind"] for item in notifications_payload["notifications"]}
            self.assertIn("collaboration_changes_requested", notification_kinds)

            self.assertEqual(main(["audit", "list", "--workspace", str(root)]), 0)
            audit_payload = json.loads((root / ".cognisync" / "audit.json").read_text(encoding="utf-8"))
            self.assertIn("collaboration", {item["event_kind"] for item in audit_payload["events"]})

            self.assertEqual(main(["usage", "report", "--workspace", str(root)]), 0)
            usage_payload = json.loads((root / ".cognisync" / "usage.json").read_text(encoding="utf-8"))
            self.assertEqual(usage_payload["summary"]["collaboration_thread_count"], 1)
            self.assertEqual(usage_payload["summary"]["collaboration_comment_count"], 0)
            self.assertEqual(usage_payload["summary"]["collaboration_decision_count"], 1)

            self.assertEqual(main(["sync", "export", "--workspace", str(root)]), 0)
            bundle_root = max((root / "outputs" / "reports" / "sync-bundles").iterdir(), key=lambda path: path.name)
            sync_payload = json.loads((bundle_root / "manifest.json").read_text(encoding="utf-8"))
            self.assertIn("collaboration", sync_payload["state_manifests"])


if __name__ == "__main__":
    unittest.main()
