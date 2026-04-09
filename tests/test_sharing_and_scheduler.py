import json
import io
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from http.client import HTTPConnection
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.control_plane import create_control_plane_server, run_scheduler_tick
from cognisync.workspace import Workspace


class SharingAndSchedulerTests(unittest.TestCase):
    def test_share_can_attach_remote_bundle_and_pull_sync_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher_root = Path(tmp) / "publisher"
            follower_root = Path(tmp) / "follower"
            bundle_path = Path(tmp) / "downstream-bundle.json"

            publisher_workspace = Workspace(publisher_root)
            publisher_workspace.initialize(name="Publisher Workspace")
            (publisher_root / "raw" / "remote-notes.md").write_text(
                "# Remote Notes\n\nPulled over the hosted control plane.\n",
                encoding="utf-8",
            )

            server = create_control_plane_server(workspace=publisher_workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                self.assertEqual(
                    main(
                        [
                            "share",
                            "bind-control-plane",
                            f"http://{host}:{port}",
                            "--workspace",
                            str(publisher_root),
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "share",
                            "invite-peer",
                            "downstream",
                            "operator",
                            "--workspace",
                            str(publisher_root),
                            "--capability",
                            "sync.import",
                        ]
                    ),
                    0,
                )
                self.assertEqual(main(["share", "accept-peer", "downstream", "--workspace", str(publisher_root)]), 0)
                self.assertEqual(
                    main(
                        [
                            "share",
                            "issue-peer-bundle",
                            "downstream",
                            "--workspace",
                            str(publisher_root),
                            "--output-file",
                            str(bundle_path),
                        ]
                    ),
                    0,
                )

                self.assertEqual(main(["init", str(follower_root), "--name", "Follower Workspace"]), 0)
                self.assertEqual(
                    main(
                        [
                            "share",
                            "attach-remote-bundle",
                            str(bundle_path),
                            "--workspace",
                            str(follower_root),
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "share",
                            "pull-remote",
                            "downstream",
                            "--workspace",
                            str(follower_root),
                        ]
                    ),
                    0,
                )

                self.assertTrue((follower_root / "raw" / "remote-notes.md").exists())
                self.assertIn(
                    "Pulled over the hosted control plane.",
                    (follower_root / "raw" / "remote-notes.md").read_text(encoding="utf-8"),
                )

                sharing_payload = json.loads((follower_root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
                self.assertEqual(len(sharing_payload["attached_remotes"]), 1)
                remote = sharing_payload["attached_remotes"][0]
                self.assertEqual(remote["principal_id"], "downstream")
                self.assertEqual(remote["status"], "attached")
                self.assertTrue(remote["last_pull_at"])
                self.assertEqual(remote["last_pull_status"], "imported")
                self.assertTrue(remote["last_imported_sync_event_path"])

                sync_history_payload = json.loads((follower_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
                self.assertEqual(sync_history_payload["events"][0]["source_peer_id"], "downstream")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_scheduler_can_enqueue_due_attached_remote_pulls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher_root = Path(tmp) / "publisher"
            follower_root = Path(tmp) / "follower"
            bundle_path = Path(tmp) / "downstream-bundle.json"

            publisher_workspace = Workspace(publisher_root)
            publisher_workspace.initialize(name="Publisher Workspace")
            (publisher_root / "raw" / "remote-graph.md").write_text(
                "# Remote Graph\n\nAttached remotes should be schedulable.\n",
                encoding="utf-8",
            )

            server = create_control_plane_server(workspace=publisher_workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                self.assertEqual(
                    main(
                        [
                            "share",
                            "bind-control-plane",
                            f"http://{host}:{port}",
                            "--workspace",
                            str(publisher_root),
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "share",
                            "invite-peer",
                            "downstream",
                            "operator",
                            "--workspace",
                            str(publisher_root),
                            "--capability",
                            "sync.import",
                        ]
                    ),
                    0,
                )
                self.assertEqual(main(["share", "accept-peer", "downstream", "--workspace", str(publisher_root)]), 0)
                self.assertEqual(
                    main(
                        [
                            "share",
                            "issue-peer-bundle",
                            "downstream",
                            "--workspace",
                            str(publisher_root),
                            "--output-file",
                            str(bundle_path),
                        ]
                    ),
                    0,
                )

                follower_workspace = Workspace(follower_root)
                follower_workspace.initialize(name="Follower Workspace")
                self.assertEqual(
                    main(
                        [
                            "share",
                            "attach-remote-bundle",
                            str(bundle_path),
                            "--workspace",
                            str(follower_root),
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "share",
                            "subscribe-remote-pull",
                            "downstream",
                            "--workspace",
                            str(follower_root),
                            "--every-hours",
                            "1",
                        ]
                    ),
                    0,
                )

                sharing_path = follower_root / ".cognisync" / "shared-workspace.json"
                sharing_payload = json.loads(sharing_path.read_text(encoding="utf-8"))
                sharing_payload["attached_remotes"][0]["pull_subscription"]["next_pull_at"] = "2000-01-01T00:00:00+00:00"
                sharing_path.write_text(json.dumps(sharing_payload, indent=2, sort_keys=True), encoding="utf-8")

                tick = run_scheduler_tick(follower_workspace, enqueue_only=True)
                self.assertIn("downstream", tick.due_remote_pull_ids)
                self.assertTrue(tick.enqueued_job_ids)

                self.assertEqual(
                    main(
                        [
                            "jobs",
                            "work",
                            "--workspace",
                            str(follower_root),
                            "--max-jobs",
                            "1",
                            "--capability",
                            "sync",
                        ]
                    ),
                    0,
                )

                self.assertTrue((follower_root / "raw" / "remote-graph.md").exists())
                queue_payload = json.loads((follower_root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                self.assertEqual(queue_payload["status_counts"]["completed"], 1)

                updated_payload = json.loads(sharing_path.read_text(encoding="utf-8"))
                remote = updated_payload["attached_remotes"][0]
                self.assertEqual(remote["pull_subscription"]["last_tick_status"], "imported")
                self.assertTrue(remote["pull_subscription"]["last_pull_at"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_share_attached_remote_lifecycle_can_refresh_suspend_and_detach_remote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            publisher_root = Path(tmp) / "publisher"
            follower_root = Path(tmp) / "follower"
            first_bundle_path = Path(tmp) / "downstream-first-bundle.json"
            refreshed_bundle_path = Path(tmp) / "downstream-refreshed-bundle.json"

            self.assertEqual(main(["init", str(publisher_root), "--name", "Publisher Workspace"]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://control-a.example.test/api",
                        "--workspace",
                        str(publisher_root),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "invite-peer",
                        "downstream",
                        "operator",
                        "--workspace",
                        str(publisher_root),
                        "--capability",
                        "sync.import",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "downstream", "--workspace", str(publisher_root)]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "issue-peer-bundle",
                        "downstream",
                        "--workspace",
                        str(publisher_root),
                        "--output-file",
                        str(first_bundle_path),
                    ]
                ),
                0,
            )

            follower_workspace = Workspace(follower_root)
            follower_workspace.initialize(name="Follower Workspace")
            self.assertEqual(
                main(
                    [
                        "share",
                        "attach-remote-bundle",
                        str(first_bundle_path),
                        "--workspace",
                        str(follower_root),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "subscribe-remote-pull",
                        "downstream",
                        "--workspace",
                        str(follower_root),
                        "--every-hours",
                        "3",
                    ]
                ),
                0,
            )

            first_payload = json.loads((follower_root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            first_remote = first_payload["attached_remotes"][0]
            first_attached_at = first_remote["attached_at"]
            first_token = first_remote["token"]
            self.assertTrue(first_remote["pull_subscription"]["enabled"])
            self.assertEqual(first_remote["pull_subscription"]["interval_hours"], 3)

            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://control-b.example.test/api",
                        "--workspace",
                        str(publisher_root),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "share",
                        "issue-peer-bundle",
                        "downstream",
                        "--workspace",
                        str(publisher_root),
                        "--output-file",
                        str(refreshed_bundle_path),
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "share",
                        "refresh-remote-bundle",
                        str(refreshed_bundle_path),
                        "--workspace",
                        str(follower_root),
                    ]
                ),
                0,
            )

            refreshed_payload = json.loads(
                (follower_root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8")
            )
            refreshed_remote = refreshed_payload["attached_remotes"][0]
            self.assertEqual(refreshed_remote["server_url"], "https://control-b.example.test/api")
            self.assertNotEqual(refreshed_remote["token"], first_token)
            self.assertEqual(refreshed_remote["attached_at"], first_attached_at)
            self.assertEqual(refreshed_remote["status"], "attached")
            self.assertTrue(refreshed_remote["pull_subscription"]["enabled"])
            self.assertEqual(refreshed_remote["pull_subscription"]["interval_hours"], 3)

            self.assertEqual(main(["share", "suspend-remote", "downstream", "--workspace", str(follower_root)]), 0)
            suspended_payload = json.loads(
                (follower_root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8")
            )
            suspended_remote = suspended_payload["attached_remotes"][0]
            self.assertEqual(suspended_remote["status"], "suspended")
            self.assertTrue(suspended_remote["suspended_at"])
            self.assertFalse(suspended_remote["pull_subscription"]["enabled"])
            self.assertEqual(suspended_remote["pull_subscription"]["next_pull_at"], "")

            tick = run_scheduler_tick(follower_workspace, enqueue_only=True)
            self.assertNotIn("downstream", tick.due_remote_pull_ids)
            self.assertEqual(tick.due_remote_pull_ids, [])

            self.assertEqual(main(["share", "detach-remote", "downstream", "--workspace", str(follower_root)]), 0)
            final_payload = json.loads((follower_root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            self.assertEqual(final_payload["attached_remotes"], [])

    def test_share_peer_lifecycle_can_update_suspend_and_remove_peer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            bundle_path = Path(tmp) / "remote-ops-bundle.json"
            self.assertEqual(main(["init", str(root), "--name", "Peer Lifecycle Workspace"]), 0)
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
                        "set-peer-role",
                        "remote-ops",
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

            sharing_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            peer = sharing_payload["peers"][0]
            self.assertEqual(peer["role"], "reviewer")
            self.assertEqual(peer["status"], "accepted")

            access_payload = json.loads((root / ".cognisync" / "access.json").read_text(encoding="utf-8"))
            member = next(item for item in access_payload["members"] if item["principal_id"] == "remote-ops")
            self.assertEqual(member["role"], "reviewer")

            self.assertEqual(main(["share", "suspend-peer", "remote-ops", "--workspace", str(root)]), 0)
            suspended_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            suspended_peer = suspended_payload["peers"][0]
            self.assertEqual(suspended_peer["status"], "suspended")
            self.assertFalse(suspended_peer["sync_subscription"]["enabled"])

            access_payload = json.loads((root / ".cognisync" / "access.json").read_text(encoding="utf-8"))
            member_ids = {item["principal_id"] for item in access_payload["members"]}
            self.assertNotIn("remote-ops", member_ids)

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "issue-peer-bundle",
                        "remote-ops",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(bundle_path),
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("must be accepted", stderr.getvalue())

            self.assertEqual(main(["share", "remove-peer", "remote-ops", "--workspace", str(root)]), 0)
            final_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            self.assertEqual(final_payload["peers"], [])

    def test_share_policy_can_disable_remote_bundles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Policy Workspace"]), 0)
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
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "set-policy",
                        "--workspace",
                        str(root),
                        "--deny-remote-workers",
                        "--deny-sync-imports",
                    ]
                ),
                0,
            )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "issue-peer-bundle",
                        "remote-ops",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(Path(tmp) / "remote-ops.json"),
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("Remote worker bundles are disabled", stderr.getvalue())

            payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            self.assertFalse(payload["trust_policy"]["allow_remote_workers"])
            self.assertFalse(payload["trust_policy"]["allow_sync_imports_from_peers"])

    def test_share_policy_can_restrict_control_plane_hosts_and_peer_roles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            trusted_bundle_path = Path(tmp) / "trusted-remote.json"
            untrusted_bundle_path = Path(tmp) / "untrusted-remote.json"
            self.assertEqual(main(["init", str(root), "--name", "Trust Policy Workspace"]), 0)

            self.assertEqual(
                main(
                    [
                        "share",
                        "set-policy",
                        "--workspace",
                        str(root),
                        "--max-peer-role",
                        "reviewer",
                        "--allow-peer-capability",
                        "review.remote",
                        "--allow-peer-capability",
                        "sync.import",
                        "--allow-control-plane-host",
                        "control.example.test",
                    ]
                ),
                0,
            )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "bind-control-plane",
                        "http://control.example.test/api",
                        "--workspace",
                        str(root),
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("requires https", stderr.getvalue())

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

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "invite-peer",
                        "remote-ops",
                        "operator",
                        "--workspace",
                        str(root),
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("max_peer_role", stderr.getvalue())

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "invite-peer",
                        "remote-jobs",
                        "reviewer",
                        "--workspace",
                        str(root),
                        "--capability",
                        "jobs.remote",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("allowed peer capability list", stderr.getvalue())

            self.assertEqual(
                main(
                    [
                        "share",
                        "invite-peer",
                        "remote-review",
                        "reviewer",
                        "--workspace",
                        str(root),
                        "--capability",
                        "review.remote",
                    ]
                ),
                0,
            )

            trusted_bundle_path.write_text(
                json.dumps(
                    {
                        "workspace_id": "trusted-workspace",
                        "workspace_name": "Trusted Workspace",
                        "server_url": "https://control.example.test/api",
                        "principal_id": "trusted-peer",
                        "display_name": "Trusted Peer",
                        "role": "reviewer",
                        "token": "cp_trusted",
                        "token_id": "token-trusted",
                        "scopes": ["control.read"],
                        "capabilities": ["sync.import"],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            untrusted_bundle_path.write_text(
                json.dumps(
                    {
                        "workspace_id": "untrusted-workspace",
                        "workspace_name": "Untrusted Workspace",
                        "server_url": "https://rogue.example.test/api",
                        "principal_id": "rogue-peer",
                        "display_name": "Rogue Peer",
                        "role": "reviewer",
                        "token": "cp_rogue",
                        "token_id": "token-rogue",
                        "scopes": ["control.read"],
                        "capabilities": ["jobs.remote"],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "share",
                        "attach-remote-bundle",
                        str(untrusted_bundle_path),
                        "--workspace",
                        str(root),
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("allowed control-plane host list", stderr.getvalue())

            self.assertEqual(
                main(
                    [
                        "share",
                        "attach-remote-bundle",
                        str(trusted_bundle_path),
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            trust_policy = payload["trust_policy"]
            self.assertEqual(trust_policy["max_peer_role"], "reviewer")
            self.assertTrue(trust_policy["require_secure_control_plane"])
            self.assertEqual(trust_policy["allowed_control_plane_hosts"], ["control.example.test"])
            self.assertEqual(trust_policy["allowed_peer_capabilities"], ["review.remote", "sync.import"])
            self.assertEqual(payload["attached_remotes"][0]["principal_id"], "trusted-peer")

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

    def test_expired_control_plane_tokens_are_rejected_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Expiring Token Workspace")

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--expires-in-hours",
                        "1",
                        "--output-file",
                        str(token_stdout),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_stdout.read_text(encoding="utf-8"))["token"]

            control_plane_path = root / ".cognisync" / "control-plane.json"
            payload = json.loads(control_plane_path.read_text(encoding="utf-8"))
            payload["tokens"][0]["expires_at"] = "2000-01-01T00:00:00+00:00"
            control_plane_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/status", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("expired", payload["error"].lower())
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
