import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.control_plane import create_control_plane_server
from cognisync.workspace import Workspace


class SharingAndSchedulerTests(unittest.TestCase):
    def test_share_manifest_tracks_bound_control_plane_and_peer_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)

            self.assertEqual(main(["init", str(root), "--name", "Shared Workspace"]), 0)
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
                        "--capability",
                        "jobs.run",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "accept-peer",
                        "remote-ops",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            sharing_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            self.assertEqual(sharing_payload["published_control_plane_url"], "https://control.example.test/api")
            self.assertEqual(len(sharing_payload["peers"]), 1)
            peer = sharing_payload["peers"][0]
            self.assertEqual(peer["peer_id"], "remote-ops")
            self.assertEqual(peer["status"], "accepted")
            self.assertEqual(peer["role"], "operator")
            self.assertEqual(peer["base_url"], "https://remote.example.test")
            self.assertEqual(peer["capabilities"], ["control.read", "jobs.run"])
            self.assertTrue(peer["accepted_at"])

            access_payload = json.loads((root / ".cognisync" / "access.json").read_text(encoding="utf-8"))
            member_ids = {item["principal_id"] for item in access_payload["members"]}
            self.assertIn("remote-ops", member_ids)

            token_stdout = Path(tmp) / "workspace-token.json"
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
                connection.request("GET", "/api/workspace", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["workspace"]["root"], workspace.root.as_posix())
                self.assertEqual(payload["sharing"]["published_control_plane_url"], "https://control.example.test/api")
                self.assertEqual(payload["sharing"]["accepted_peer_count"], 1)
                self.assertEqual(payload["sharing"]["peer_ids"], ["remote-ops"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_weekly_connector_subscription_is_visible_through_scheduler_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Scheduler Workspace")

            connector_url = "data:text/html;charset=utf-8," + quote(
                "<html><head><title>Weekly Source</title></head><body><p>Scheduled sync.</p></body></html>"
            )
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
                        "weekly-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-weekly-source",
                        "--workspace",
                        str(root),
                        "--weekday",
                        "mon",
                        "--weekday",
                        "wed",
                        "--hour",
                        "9",
                        "--minute",
                        "30",
                    ]
                ),
                0,
            )

            connectors_path = root / ".cognisync" / "connectors.json"
            connectors_payload = json.loads(connectors_path.read_text(encoding="utf-8"))
            subscription = connectors_payload["connectors"][0]["subscription"]
            self.assertEqual(subscription["schedule_type"], "weekly")
            self.assertEqual(subscription["weekdays"], ["mon", "wed"])
            self.assertEqual(subscription["hour"], 9)
            self.assertEqual(subscription["minute"], 30)
            self.assertTrue(subscription["next_sync_at"])
            self.assertIsNone(subscription["interval_hours"])

            connectors_payload["connectors"][0]["subscription"]["next_sync_at"] = "2000-01-01T00:00:00+00:00"
            connectors_path.write_text(json.dumps(connectors_payload, indent=2, sort_keys=True), encoding="utf-8")

            token_stdout = Path(tmp) / "scheduler-token.json"
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
                self.assertIn("url-weekly-source", payload["due_connector_ids"])
                self.assertEqual(payload["history"], [])
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
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/scheduler", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreaterEqual(len(payload["history"]), 1)
                self.assertEqual(payload["history"][0]["action"], "enqueued")
                self.assertIn("url-weekly-source", payload["history"][0]["due_connector_ids"])
                connection.close()

                control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
                self.assertGreaterEqual(len(control_payload["scheduler"]["history"]), 1)
                self.assertEqual(control_payload["scheduler"]["history"][0]["action"], "enqueued")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
