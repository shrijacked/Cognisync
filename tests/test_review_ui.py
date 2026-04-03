import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from http.client import HTTPConnection
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.review_ui import create_review_ui_server
from cognisync.workspace import Workspace


class ReviewUiTests(unittest.TestCase):
    def test_ui_review_writes_dashboard_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Review UI Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Systems\n\n"
                "## Agent Memory\n\n"
                "Agent Memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "queries" / "agent-memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Agent Memory\n\n"
                "Operator note without backlinks yet.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            review_id = "conflict:raw-cloud.md:raw-local.md:the deployment model:is"
            self.assertEqual(
                main(["review", "dismiss", review_id, "--reason", "tracking this manually", "--workspace", str(root)]),
                0,
            )
            self.assertEqual(
                main(["research", "--workspace", str(root), "how do agent loops use memory"]),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["ui", "review", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            html_path = workspace.review_ui_dir / "index.html"
            export_path = workspace.review_ui_dir / "review-export.json"
            state_path = workspace.review_ui_dir / "dashboard-state.json"
            self.assertTrue(html_path.exists())
            self.assertTrue(export_path.exists())
            self.assertTrue(state_path.exists())

            html = html_path.read_text(encoding="utf-8")
            payload = json.loads(export_path.read_text(encoding="utf-8"))
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertIn("Cognisync Review UI", html)
            self.assertIn("Open Review Items", html)
            self.assertIn("Dismissed Review Items", html)
            self.assertIn("Graph Overview", html)
            self.assertIn("Run History", html)
            self.assertIn(".cognisync/graph.json", html)
            self.assertIn("how do agent loops use memory", html)
            self.assertEqual(state["schema_version"], 1)
            self.assertGreaterEqual(state["graph"]["node_count"], 1)
            self.assertGreaterEqual(state["graph"]["edge_count"], 1)
            self.assertTrue(any(item["run_kind"] == "research" for item in state["runs"]["items"]))
            self.assertEqual(payload["summary"]["dismissed_item_count"], 1)
            self.assertGreaterEqual(payload["summary"]["open_item_count"], 1)
            self.assertIn("Wrote review UI to", stdout.getvalue())
            self.assertIn("Wrote review UI state to", stdout.getvalue())

    def test_review_ui_server_serves_dashboard_and_export_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Review UI Server Test")

            review_ui_dir = workspace.review_ui_dir
            review_ui_dir.mkdir(parents=True, exist_ok=True)
            (review_ui_dir / "dashboard.html").write_text("<html><body>hello ui</body></html>", encoding="utf-8")
            (review_ui_dir / "review-export.json").write_text('{"schema_version": 1}', encoding="utf-8")

            server = create_review_ui_server(review_ui_dir, host="127.0.0.1", port=0, index_name="dashboard.html")
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/")
                response = connection.getresponse()
                html = response.read().decode("utf-8")
                self.assertEqual(response.status, 200)
                self.assertIn("hello ui", html)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/review-export.json")
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["schema_version"], 1)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
