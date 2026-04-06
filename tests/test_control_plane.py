import json
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.control_plane import create_control_plane_server
from cognisync.remote_worker import run_remote_worker
from cognisync.workspace import Workspace


class ControlPlaneTests(unittest.TestCase):
    def test_share_issue_peer_bundle_writes_remote_operator_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            bundle_path = Path(tmp) / "remote-ops-bundle.json"
            self.assertEqual(main(["init", str(root), "--name", "Shared Operator Workspace"]), 0)
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
                        "https://remote.example.test/cognisync",
                        "--capability",
                        "jobs.remote",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "issue-peer-bundle",
                        "remote-ops",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(bundle_path),
                    ]
                ),
                0,
            )

            bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
            self.assertEqual(bundle_payload["principal_id"], "remote-ops")
            self.assertEqual(bundle_payload["role"], "operator")
            self.assertEqual(bundle_payload["server_url"], "https://control.example.test/api")
            self.assertEqual(bundle_payload["workspace_name"], "Shared Operator Workspace")
            self.assertIn("jobs.remote", bundle_payload["capabilities"])
            self.assertTrue(bundle_payload["token"].startswith("cp_"))
            self.assertIn("jobs.run", bundle_payload["scopes"])

            sharing_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            peer = sharing_payload["peers"][0]
            self.assertEqual(peer["peer_id"], "remote-ops")
            self.assertTrue(peer["last_bundle_issued_at"])
            self.assertTrue(peer["last_token_id"])

            control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
            remote_tokens = [item for item in control_payload["tokens"] if item["principal_id"] == "remote-ops"]
            self.assertEqual(len(remote_tokens), 1)
            self.assertEqual(remote_tokens[0]["token_id"], peer["last_token_id"])

    def test_control_plane_tracks_invites_and_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Control Plane Workspace"]), 0)

            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "invite",
                        "reviewer-2",
                        "reviewer",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "accept-invite",
                        "reviewer-2",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )
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
                        "--scope",
                        "jobs.run",
                    ]
                ),
                0,
            )

            payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
            self.assertEqual(len(payload["invites"]), 1)
            self.assertEqual(payload["invites"][0]["status"], "accepted")
            self.assertEqual(payload["invites"][0]["principal_id"], "reviewer-2")
            self.assertEqual(len(payload["tokens"]), 1)
            self.assertEqual(payload["tokens"][0]["principal_id"], "local-operator")
            self.assertEqual(payload["tokens"][0]["status"], "active")
            self.assertEqual(payload["tokens"][0]["scopes"], ["control.read", "jobs.run"])

            access_payload = json.loads((root / ".cognisync" / "access.json").read_text(encoding="utf-8"))
            member_ids = {item["principal_id"] for item in access_payload["members"]}
            self.assertIn("reviewer-2", member_ids)

    def test_control_plane_server_and_remote_worker_execute_scheduled_connector_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Remote Control Plane Workspace")

            connector_url = "data:text/html;charset=utf-8,<html><head><title>Scheduled Source</title></head><body><p>Scheduled sync.</p></body></html>"
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
                        "scheduled-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-scheduled-source",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "1",
                    ]
                ),
                0,
            )

            connectors_path = root / ".cognisync" / "connectors.json"
            connectors_payload = json.loads(connectors_path.read_text(encoding="utf-8"))
            connectors_payload["connectors"][0]["subscription"]["next_sync_at"] = "2000-01-01T00:00:00+00:00"
            connectors_path.write_text(json.dumps(connectors_payload, indent=2, sort_keys=True), encoding="utf-8")

            token_stdout = Path(tmp) / "token.txt"
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
                        "--scope",
                        "scheduler.run",
                        "--scope",
                        "jobs.run",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
                        "--output-file",
                        str(token_stdout),
                    ]
                ),
                0,
            )
            token_payload = json.loads(token_stdout.read_text(encoding="utf-8"))
            token_value = token_payload["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/status", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                status_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(status_payload["workspace"]["root"], workspace.root.as_posix())
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                body = json.dumps({"enqueue_only": True})
                connection.request(
                    "POST",
                    "/api/scheduler/tick",
                    body=body,
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                tick_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreaterEqual(tick_payload["due_connector_count"], 1)
                self.assertEqual(tick_payload["action"], "enqueued")
                connection.close()

                self.assertEqual(
                    main(
                        [
                            "worker",
                            "remote",
                            "--server-url",
                            f"http://{host}:{port}",
                            "--token",
                            token_value,
                            "--worker-id",
                            "remote-alpha",
                            "--max-jobs",
                            "1",
                        ]
                    ),
                    0,
                )

                queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                self.assertEqual(queue_payload["queued_count"], 0)
                self.assertTrue((root / "raw" / "urls" / "scheduled-source.md").exists())

                control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
                scheduler = control_payload["scheduler"]
                self.assertEqual(scheduler["last_action"], "enqueued")
                self.assertGreaterEqual(len(scheduler["last_due_connector_ids"]), 1)
                self.assertGreaterEqual(len(scheduler["last_enqueued_job_ids"]), 1)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_can_poll_for_future_jobs_and_workers_are_visible_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Polling Worker Workspace")
            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\nAgent memory benefits from explicit links.\n",
                encoding="utf-8",
            )

            token_stdout = Path(tmp) / "token.json"
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
                        "--scope",
                        "jobs.run",
                        "--output-file",
                        str(token_stdout),
                    ]
                ),
                0,
            )
            token_payload = json.loads(token_stdout.read_text(encoding="utf-8"))
            token_value = token_payload["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                worker_result = {}

                def _run_worker() -> None:
                    worker_result["result"] = run_remote_worker(
                        server_url=f"http://{host}:{port}",
                        token=token_value,
                        worker_id="remote-poller",
                        max_jobs=1,
                        poll_interval_seconds=0.1,
                        max_idle_polls=10,
                    )

                worker_thread = threading.Thread(target=_run_worker, daemon=True)
                worker_thread.start()
                time.sleep(0.2)

                self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)

                worker_thread.join(timeout=5)
                self.assertFalse(worker_thread.is_alive(), "remote worker did not finish after polling")
                self.assertEqual(worker_result["result"].processed_count, 1)
                self.assertEqual(worker_result["result"].completed_count, 1)

                queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                self.assertEqual(queue_payload["queued_count"], 0)

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/workers", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                workers_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(workers_payload["counts_by_status"]["idle"], 1)
                self.assertEqual(workers_payload["workers"][0]["worker_id"], "remote-poller")
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_scheduler_can_enqueue_due_peer_sync_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Scheduled Peer Sync Workspace")
            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
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
                        "--capability",
                        "sync.import",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "subscribe-sync",
                        "remote-ops",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "1",
                    ]
                ),
                0,
            )

            sharing_path = root / ".cognisync" / "shared-workspace.json"
            sharing_payload = json.loads(sharing_path.read_text(encoding="utf-8"))
            sharing_payload["peers"][0]["sync_subscription"]["next_sync_at"] = "2000-01-01T00:00:00+00:00"
            sharing_path.write_text(json.dumps(sharing_payload, indent=2, sort_keys=True), encoding="utf-8")

            token_stdout = Path(tmp) / "token.json"
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
                        "--scope",
                        "scheduler.run",
                        "--scope",
                        "jobs.run",
                        "--output-file",
                        str(token_stdout),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_stdout.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/scheduler", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertIn("remote-ops", payload["due_peer_sync_ids"])
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/scheduler/tick",
                    body=json.dumps({"enqueue_only": True}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                tick_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(tick_payload["action"], "enqueued")
                self.assertIn("remote-ops", tick_payload["due_peer_sync_ids"])
                connection.close()

                self.assertEqual(
                    main(
                        [
                            "worker",
                            "remote",
                            "--server-url",
                            f"http://{host}:{port}",
                            "--token",
                            token_value,
                            "--worker-id",
                            "remote-syncer",
                            "--max-jobs",
                            "1",
                        ]
                    ),
                    0,
                )

                bundle_dirs = sorted((root / "outputs" / "reports" / "sync-bundles").glob("sync-bundle-*"))
                self.assertTrue(bundle_dirs)
                manifest_payload = json.loads((bundle_dirs[-1] / "manifest.json").read_text(encoding="utf-8"))
                self.assertEqual(manifest_payload["shared_peer"]["peer_id"], "remote-ops")

                control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
                self.assertIn("remote-ops", control_payload["scheduler"]["last_due_peer_sync_ids"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
