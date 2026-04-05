import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.control_plane import create_control_plane_server
from cognisync.workspace import Workspace


class ControlPlaneTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
