import io
import json
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from http.client import HTTPConnection
from pathlib import Path
from urllib.parse import urlencode

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
            connector_url = "data:text/html;charset=utf-8,<html><head><title>Connector Page</title></head><body><p>Connector body.</p></body></html>"
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        connector_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "connector-page",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "research",
                        "--workspace",
                        str(root),
                        "map the open questions in this corpus",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["sync", "export", "--workspace", str(root)]), 0)
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
            self.assertIn("Job Queue", html)
            self.assertIn("Sync History", html)
            self.assertIn("Connectors", html)
            self.assertIn("Graph Node Explorer", html)
            self.assertIn("Run Explorer", html)
            self.assertIn("Job Explorer", html)
            self.assertIn("Sync Explorer", html)
            self.assertIn("Connector Explorer", html)
            self.assertIn("Source Coverage", html)
            self.assertIn("Compile Health", html)
            self.assertIn("Run Timeline", html)
            self.assertIn("Concept Graph", html)
            self.assertIn("Filter nodes", html)
            self.assertIn("Filter runs", html)
            self.assertIn(".cognisync/graph.json", html)
            self.assertIn(".cognisync/jobs/queue.json", html)
            self.assertIn(".cognisync/sync/history.json", html)
            self.assertIn(".cognisync/connectors.json", html)
            self.assertIn("how do agent loops use memory", html)
            self.assertEqual(state["schema_version"], 1)
            self.assertGreaterEqual(state["graph"]["node_count"], 1)
            self.assertGreaterEqual(state["graph"]["edge_count"], 1)
            self.assertGreaterEqual(state["source_coverage"]["source_count"], 1)
            self.assertGreaterEqual(state["compile_health"]["pending_task_count"], 1)
            self.assertGreaterEqual(state["run_timeline"]["total_count"], 1)
            self.assertGreaterEqual(state["concept_graph"]["selected_node_count"], 1)
            self.assertGreaterEqual(state["jobs"]["total_count"], 1)
            self.assertGreaterEqual(state["sync"]["total_count"], 1)
            self.assertGreaterEqual(state["connectors"]["total_count"], 1)
            self.assertGreaterEqual(len(state["graph"]["nodes"]), 1)
            self.assertTrue(any(item["detail_href"] for item in state["graph"]["nodes"]))
            self.assertTrue(any(item["run_kind"] == "research" for item in state["runs"]["items"]))
            self.assertTrue(any(item["detail_href"] for item in state["runs"]["items"]))
            self.assertTrue(any(item["detail_href"] for item in state["jobs"]["items"]))
            self.assertTrue(any(item["detail_href"] for item in state["sync"]["items"]))
            self.assertTrue(any(item["detail_href"] for item in state["connectors"]["items"]))
            self.assertEqual(payload["summary"]["dismissed_item_count"], 1)
            self.assertGreaterEqual(payload["summary"]["open_item_count"], 1)
            graph_detail_href = state["graph"]["nodes"][0]["detail_href"]
            run_detail_href = next(item["detail_href"] for item in state["runs"]["items"] if item["run_kind"] == "research")
            job_detail_href = state["jobs"]["items"][0]["detail_href"]
            sync_detail_href = state["sync"]["items"][0]["detail_href"]
            connector_detail_href = state["connectors"]["items"][0]["detail_href"]
            change_detail_href = state["change_summaries"][0]["detail_href"]
            concept_graph_href = state["concept_graph"]["map_href"]
            run_timeline_href = state["run_timeline"]["detail_href"]
            graph_detail_path = workspace.review_ui_dir / graph_detail_href
            run_detail_path = workspace.review_ui_dir / run_detail_href
            job_detail_path = workspace.review_ui_dir / job_detail_href
            sync_detail_path = workspace.review_ui_dir / sync_detail_href
            connector_detail_path = workspace.review_ui_dir / connector_detail_href
            change_detail_path = workspace.review_ui_dir / change_detail_href
            concept_graph_path = workspace.review_ui_dir / concept_graph_href
            run_timeline_path = workspace.review_ui_dir / run_timeline_href
            self.assertTrue(graph_detail_path.exists())
            self.assertTrue(run_detail_path.exists())
            self.assertTrue(job_detail_path.exists())
            self.assertTrue(sync_detail_path.exists())
            self.assertTrue(connector_detail_path.exists())
            self.assertTrue(change_detail_path.exists())
            self.assertTrue(concept_graph_path.exists())
            self.assertTrue(run_timeline_path.exists())
            self.assertIn("Graph Node Detail", graph_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Run Detail", run_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Job Detail", job_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Sync Detail", sync_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Connector Detail", connector_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Artifact Preview", change_detail_path.read_text(encoding="utf-8"))
            self.assertIn("Concept Graph", concept_graph_path.read_text(encoding="utf-8"))
            self.assertIn("Run Timeline", run_timeline_path.read_text(encoding="utf-8"))
            self.assertIn(graph_detail_href, html)
            self.assertIn(run_detail_href, html)
            self.assertIn(job_detail_href, html)
            self.assertIn(sync_detail_href, html)
            self.assertIn(connector_detail_href, html)
            self.assertIn(change_detail_href, html)
            self.assertIn(concept_graph_href, html)
            self.assertIn(run_timeline_href, html)
            self.assertIn("action=\"/api/review/dismiss\"", html)
            self.assertIn("action=\"/api/jobs/run-next\"", html)
            self.assertIn("action=\"/api/connectors/sync\"", html)
            self.assertIn("action=\"/api/connectors/sync-all\"", html)
            self.assertIn("action=\"/api/review/reopen\"", html)
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

    def test_review_ui_server_applies_actions_and_refreshes_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Review UI Action Test")

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

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            connector_url = "data:text/html;charset=utf-8,<html><head><title>Server Connector</title></head><body><p>Connector body.</p></body></html>"
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        connector_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "server-connector",
                    ]
                ),
                0,
            )
            second_connector_url = "data:text/html;charset=utf-8,<html><head><title>Batch Connector</title></head><body><p>Second connector body.</p></body></html>"
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_connector_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-connector",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(main(["ui", "review", "--workspace", str(root)]), 0)

            server = create_review_ui_server(
                workspace.review_ui_dir,
                host="127.0.0.1",
                port=0,
                index_name="index.html",
                workspace=workspace,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                body = urlencode({"review_id": "concept-candidate:agents", "reason": "later"})
                connection.request(
                    "POST",
                    "/api/review/dismiss",
                    body=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                state = json.loads((workspace.review_ui_dir / "dashboard-state.json").read_text(encoding="utf-8"))
                self.assertEqual(state["review"]["summary"]["dismissed_item_count"], 1)

                connection = HTTPConnection(host, port, timeout=5)
                body = urlencode({"review_id": "concept-candidate:agents"})
                connection.request(
                    "POST",
                    "/api/review/reopen",
                    body=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                state = json.loads((workspace.review_ui_dir / "dashboard-state.json").read_text(encoding="utf-8"))
                self.assertEqual(state["review"]["summary"]["dismissed_item_count"], 0)

                connection = HTTPConnection(host, port, timeout=5)
                body = urlencode({"slug": "agent-memory"})
                connection.request(
                    "POST",
                    "/api/review/accept-concept",
                    body=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                self.assertTrue((workspace.wiki_dir / "concepts" / "agent-memory.md").exists())
                html = (workspace.review_ui_dir / "index.html").read_text(encoding="utf-8")
                self.assertIn("action=\"/api/review/accept-concept\"", html)

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/run-next",
                    body="",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                queue_payload = json.loads((workspace.jobs_dir / "queue.json").read_text(encoding="utf-8"))
                self.assertEqual(queue_payload["queued_count"], 0)

                connection = HTTPConnection(host, port, timeout=5)
                body = urlencode({"connector_id": "url-server-connector"})
                connection.request(
                    "POST",
                    "/api/connectors/sync",
                    body=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                self.assertTrue((workspace.raw_dir / "urls" / "server-connector.md").exists())

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/sync-all",
                    body="",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                response = connection.getresponse()
                response.read()
                self.assertEqual(response.status, 303)
                connection.close()

                self.assertTrue((workspace.raw_dir / "urls" / "batch-connector.md").exists())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
