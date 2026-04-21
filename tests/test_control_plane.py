import io
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.client import HTTPConnection
from pathlib import Path
from unittest import mock

from tests import support  # noqa: F401

import cognisync.remote_worker as remote_worker_module
from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config
from cognisync.control_plane import create_control_plane_server
from cognisync.hosted_hardening import build_hosted_hardening_report
from cognisync.remote_worker import run_remote_worker
from cognisync.sync import encode_sync_bundle_archive, export_sync_bundle, import_sync_bundle_archive
from cognisync.workspace import Workspace


class ControlPlaneTests(unittest.TestCase):
    def test_control_plane_status_surfaces_hosted_hardening_findings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Hosted Hardening Workspace"]), 0)
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(main(["control-plane", "status", "--workspace", str(root)]), 0)

            output = stdout.getvalue()
            self.assertIn("## Hosted Hardening", output)
            self.assertIn("- Status: `attention`", output)
            self.assertIn("token-no-expiry:local-operator", output)
            self.assertIn("token-operator-no-expiry:local-operator", output)
            self.assertIn("trust-policy-peer-capabilities-any", output)
            self.assertIn("Re-issue the token with `--expires-in-hours`", output)

    def test_control_plane_status_api_includes_hosted_hardening_without_token_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Hardening API Workspace")

            token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(token_path),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/status", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                raw_body = response.read().decode("utf-8")
                payload = json.loads(raw_body)
                self.assertEqual(response.status, 200)
                connection.close()

                hardening = payload["hardening"]
                self.assertEqual(hardening["status"], "attention")
                self.assertGreaterEqual(hardening["summary"]["finding_count"], 3)
                finding_ids = {item["finding_id"] for item in hardening["findings"]}
                self.assertIn("token-no-expiry:local-operator", finding_ids)
                self.assertIn("token-operator-no-expiry:local-operator", finding_ids)
                self.assertIn("trust-policy-peer-capabilities-any", finding_ids)
                self.assertNotIn(token_value, raw_body)
                self.assertNotIn("token_hash", raw_body)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_hosted_hardening_report_detects_operational_risk_classes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            self.assertEqual(main(["init", str(root), "--name", "Hosted Hardening Risk Workspace"]), 0)
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
                    ]
                ),
                0,
            )

            control_payload = json.loads(workspace.control_plane_manifest_path.read_text(encoding="utf-8"))
            control_payload["tokens"][0]["expires_at"] = "2000-01-01T00:00:00Z"
            workspace.control_plane_manifest_path.write_text(
                json.dumps(control_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            workspace.worker_registry_path.parent.mkdir(parents=True, exist_ok=True)
            workspace.worker_registry_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "workers": [
                            {
                                "worker_id": "stale-worker",
                                "status": "running",
                                "last_seen_at": "2000-01-01T00:00:00Z",
                                "current_job_id": "running-job",
                            }
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            workspace.job_queue_manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "queued_count": 1,
                        "jobs": [
                            {"job_id": "queued-job", "status": "queued"},
                            {"job_id": "failed-job", "status": "failed", "title": "Broken mirror"},
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            report = build_hosted_hardening_report(workspace)

            finding_ids = {str(item["finding_id"]) for item in report["findings"]}
            self.assertIn("token-expired-active:local-operator", finding_ids)
            self.assertIn("worker-stale-active:stale-worker", finding_ids)
            self.assertIn("job-queue-backlog", finding_ids)
            self.assertIn("notification-high:job_failed", finding_ids)
            self.assertGreaterEqual(report["summary"]["high_count"], 3)
            self.assertNotIn("token_hash", json.dumps(report))

    def test_control_plane_exposes_shared_workspace_access_and_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Read Surface Workspace")
            (root / "outputs" / "reports").mkdir(parents=True, exist_ok=True)
            (root / "outputs" / "reports" / "artifact.md").write_text("# Artifact\n\nPending review.\n", encoding="utf-8")

            self.assertEqual(
                main(["access", "grant", "editor-1", "editor", "--workspace", str(root)]),
                0,
            )
            self.assertEqual(
                main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]),
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
                        "collab",
                        "request-review",
                        "outputs/reports/artifact.md",
                        "--workspace",
                        str(root),
                        "--assign",
                        "reviewer-1",
                        "--actor-id",
                        "editor-1",
                    ]
                ),
                0,
            )

            token_stdout = Path(tmp) / "reader-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "editor-1",
                        "--workspace",
                        str(root),
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
                connection.request("GET", "/api/share", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["accepted_peer_count"], 1)
                self.assertEqual(payload["peers"][0]["peer_id"], "remote-ops")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/access", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["member_count"], 4)
                member_ids = {item["principal_id"] for item in payload["members"]}
                self.assertIn("editor-1", member_ids)
                self.assertIn("reviewer-1", member_ids)
                self.assertIn("remote-ops", member_ids)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/collab", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["thread_count"], 1)
                self.assertEqual(payload["threads"][0]["status"], "pending_review")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/notifications", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                notification_kinds = {item["kind"] for item in payload["notifications"]}
                self.assertIn("collaboration_pending_review", notification_kinds)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_manage_attached_remotes_over_http(self) -> None:
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
            self.assertEqual(main(["init", str(follower_root), "--name", "Follower Workspace"]), 0)

            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://follower-control.example.test/api",
                        "--workspace",
                        str(follower_root),
                    ]
                ),
                0,
            )
            token_path = Path(tmp) / "follower-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(follower_root),
                        "--scope",
                        "control.read",
                        "--output-file",
                        str(token_path),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_path.read_text(encoding="utf-8"))["token"]

            follower_workspace = Workspace(follower_root)
            server = create_control_plane_server(workspace=follower_workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                first_bundle = json.loads(first_bundle_path.read_text(encoding="utf-8"))

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/remotes/attach",
                    body=json.dumps({"bundle": first_bundle}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["remote"]["principal_id"], "downstream")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/share", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(len(payload["attached_remotes"]), 1)
                first_remote = payload["attached_remotes"][0]
                first_attached_at = first_remote["attached_at"]
                first_remote_id = first_remote["remote_id"]
                first_token = first_remote["token"]
                connection.close()

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
                refreshed_bundle = json.loads(refreshed_bundle_path.read_text(encoding="utf-8"))

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/remotes/refresh",
                    body=json.dumps({"bundle": refreshed_bundle}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["remote"]["server_url"], "https://control-b.example.test/api")
                self.assertNotEqual(payload["remote"]["token"], first_token)
                self.assertEqual(payload["remote"]["attached_at"], first_attached_at)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/remotes/suspend",
                    body=json.dumps({"remote_ref": "downstream"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["remote"]["status"], "suspended")
                self.assertFalse(payload["remote"]["pull_subscription"]["enabled"])
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/remotes/remove",
                    body=json.dumps({"remote_ref": "downstream"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["removed_remote_id"], first_remote_id)
                self.assertEqual(payload["sharing"]["attached_remote_count"], 0)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_collaboration_actions_respect_workspace_roles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Collaboration Workspace")
            (root / "outputs" / "reports").mkdir(parents=True, exist_ok=True)
            (root / "outputs" / "reports" / "artifact.md").write_text("# Artifact\n\nNeeds review.\n", encoding="utf-8")

            self.assertEqual(main(["access", "grant", "editor-1", "editor", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "viewer-1", "viewer", "--workspace", str(root)]), 0)

            editor_token_path = Path(tmp) / "editor-token.json"
            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            viewer_token_path = Path(tmp) / "viewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "editor-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(editor_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "viewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(viewer_token_path),
                    ]
                ),
                0,
            )
            editor_token = json.loads(editor_token_path.read_text(encoding="utf-8"))["token"]
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]
            viewer_token = json.loads(viewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/collab/request-review",
                    body=json.dumps(
                        {
                            "artifact_path": "outputs/reports/artifact.md",
                            "assignee_ids": ["reviewer-1"],
                            "note": "please review",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {editor_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["thread"]["status"], "pending_review")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/collab/comment",
                    body=json.dumps(
                        {
                            "artifact_path": "outputs/reports/artifact.md",
                            "message": "looks mostly good",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["comment"]["actor"]["principal_id"], "reviewer-1")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/collab/request-changes",
                    body=json.dumps(
                        {
                            "artifact_path": "outputs/reports/artifact.md",
                            "summary": "cite the supporting source",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["decision"]["decision"], "changes_requested")
                self.assertEqual(payload["thread"]["status"], "changes_requested")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/collab/resolve",
                    body=json.dumps({"artifact_path": "outputs/reports/artifact.md"}),
                    headers={
                        "Authorization": f"Bearer {editor_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["thread"]["status"], "resolved")
                self.assertEqual(payload["thread"]["resolved_by"]["principal_id"], "editor-1")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/collab/request-review",
                    body=json.dumps({"artifact_path": "outputs/reports/artifact.md"}),
                    headers={
                        "Authorization": f"Bearer {viewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("does not have permission", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_exposes_review_queue_and_dismissal_state_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Review Read Surface Workspace")

            (workspace.raw_dir / "retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Systems\n\n"
                "## Agent Memory\n\n"
                "Agent Memory benefits from explicit links.\n",
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
            (workspace.wiki_dir / "queries" / "agent-memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Agent Memory\n\n"
                "Operator note without backlinks yet.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/review", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreaterEqual(payload["summary"]["open_item_count"], 2)
                open_review_ids = {item["review_id"] for item in payload["open_items"]}
                self.assertIn("backlink:wiki-queries-agent-memory.md:raw-retrieval.md", open_review_ids)
                self.assertIn("conflict:raw-cloud.md:raw-local.md:the deployment model:is", open_review_ids)
                self.assertFalse(payload["dismissed_items"])
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/dismiss",
                    body=json.dumps(
                        {
                            "review_id": "backlink:wiki-queries-agent-memory.md:raw-retrieval.md",
                            "reason": "tracked in another navigation note",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["dismissed"]["reason"], "tracked in another navigation note")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/review", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["dismissed_item_count"], 1)
                self.assertEqual(payload["dismissed_items"][0]["review_id"], "backlink:wiki-queries-agent-memory.md:raw-retrieval.md")
                open_review_ids = {item["review_id"] for item in payload["open_items"]}
                self.assertNotIn("backlink:wiki-queries-agent-memory.md:raw-retrieval.md", open_review_ids)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/reopen",
                    body=json.dumps({"review_id": "backlink:wiki-queries-agent-memory.md:raw-retrieval.md"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["reopened"]["reason"], "tracked in another navigation note")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/review", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["dismissed_item_count"], 0)
                open_review_ids = {item["review_id"] for item in payload["open_items"]}
                self.assertIn("backlink:wiki-queries-agent-memory.md:raw-retrieval.md", open_review_ids)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_apply_review_actions_over_http_and_enforce_review_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Review Action Workspace")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Database\n\nVector Database improves recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "runtimes-a.md").write_text(
                "# Runtime Systems\n\n## Agent Runtimes\n\nAgent Runtimes coordinate tools.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "runtimes-b.md").write_text(
                "# Runtime Notes\n\n## Agent Runtimes\n\nAgent Runtimes coordinate tools.\n",
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
            (workspace.raw_dir / "tagged-retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Tags\n\n"
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
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "viewer-1", "viewer", "--workspace", str(root)]), 0)

            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            viewer_token_path = Path(tmp) / "viewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "viewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(viewer_token_path),
                    ]
                ),
                0,
            )
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]
            viewer_token = json.loads(viewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/accept-concept",
                    body=json.dumps({"slug": "agent-runtimes"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["concept_path"].endswith("wiki/concepts/agent-runtimes.md"))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/resolve-merge",
                    body=json.dumps({"canonical_label": "vector database"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["concept_path"].endswith("wiki/concepts/vector-databases.md"))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/apply-backlink",
                    body=json.dumps({"target_path": "wiki/queries/agent-memory.md"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["path"].endswith("wiki/queries.md"))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/file-conflict",
                    body=json.dumps({"subject": "the deployment model"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["note_path"].endswith("wiki/queries/conflicts/the-deployment-model.md"))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/review/dismiss",
                    body=json.dumps(
                        {
                            "review_id": "backlink:wiki-queries-agent-memory.md:raw-tagged-retrieval.md",
                            "reason": "viewer should not be able to do this",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {viewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("review.run", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_exposes_run_sync_and_change_summary_history_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted History Surface Workspace")
            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["researcher"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use structured memory to retain findings. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "researcher",
                        "--mode",
                        "memo",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["sync", "export", "--workspace", str(root)]), 0)
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/runs", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreaterEqual(payload["summary"]["total_count"], 1)
                self.assertTrue(any(item["run_kind"] == "research" for item in payload["items"]))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/sync", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["total_count"], 1)
                self.assertEqual(payload["items"][0]["operation"], "export")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/change-summaries", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreaterEqual(payload["summary"]["total_count"], 2)
                triggers = {item["trigger"] for item in payload["items"]}
                self.assertIn("scan", triggers)
                self.assertIn("research", triggers)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_manage_shared_sync_policy_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Share Policy Workspace")

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
                        "reviewer",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)

            token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(token_path),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/set-policy",
                    body=json.dumps(
                        {
                            "allow_remote_workers": False,
                            "allow_sync_imports_from_peers": False,
                            "max_peer_role": "reviewer",
                            "require_secure_control_plane": True,
                            "allowed_control_plane_hosts": ["control.example.test"],
                            "allowed_peer_capabilities": ["review.remote", "sync.import"],
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertFalse(payload["trust_policy"]["allow_remote_workers"])
                self.assertFalse(payload["trust_policy"]["allow_sync_imports_from_peers"])
                self.assertEqual(payload["trust_policy"]["max_peer_role"], "reviewer")
                self.assertTrue(payload["trust_policy"]["require_secure_control_plane"])
                self.assertEqual(payload["trust_policy"]["allowed_control_plane_hosts"], ["control.example.test"])
                self.assertEqual(payload["trust_policy"]["allowed_peer_capabilities"], ["review.remote", "sync.import"])
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/subscribe-sync",
                    body=json.dumps({"peer_ref": "remote-ops", "every_hours": 1}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["peer"]["sync_subscription"]["enabled"])
                self.assertEqual(payload["peer"]["sync_subscription"]["interval_hours"], 1)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/unsubscribe-sync",
                    body=json.dumps({"peer_ref": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertFalse(payload["peer"]["sync_subscription"]["enabled"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_manage_shared_peer_lifecycle_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Peer Lifecycle Workspace")

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

            token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(token_path),
                    ]
                ),
                0,
            )
            token_value = json.loads(token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/peers/role",
                    body=json.dumps({"peer_id": "remote-ops", "role": "reviewer"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["peer"]["role"], "reviewer")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/peers/suspend",
                    body=json.dumps({"peer_id": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["peer"]["status"], "suspended")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/peers/remove",
                    body=json.dumps({"peer_id": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["removed_peer_id"], "remote-ops")
                self.assertEqual(payload["sharing"]["peer_count"], 0)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_manage_access_invites_and_tokens_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Auth Admin Workspace")

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/access/grant",
                    body=json.dumps(
                        {
                            "principal_id": "reviewer-2",
                            "role": "reviewer",
                            "display_name": "Reviewer Two",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["member"]["principal_id"], "reviewer-2")
                self.assertEqual(payload["member"]["role"], "reviewer")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/invites/create",
                    body=json.dumps({"principal_id": "editor-2", "role": "editor"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["invite"]["principal_id"], "editor-2")
                self.assertEqual(payload["invite"]["status"], "pending")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/invites/accept",
                    body=json.dumps({"invite_ref": "editor-2"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["invite"]["status"], "accepted")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/tokens/issue",
                    body=json.dumps({"principal_id": "reviewer-2"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["token"].startswith("cp_"))
                reviewer_token_id = payload["token_metadata"]["token_id"]
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/tokens", headers={"Authorization": f"Bearer {operator_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(any(item["token_id"] == reviewer_token_id for item in payload["tokens"]))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/invites", headers={"Authorization": f"Bearer {operator_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(any(item["principal_id"] == "editor-2" for item in payload["invites"]))
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/tokens/revoke",
                    body=json.dumps({"token_id": reviewer_token_id}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["token"]["status"], "revoked")
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_inspect_and_run_connectors_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Connector Surface Workspace")

            connector_url = "data:text/html;charset=utf-8,<html><head><title>Connector Source</title></head><body><p>Remote sync.</p></body></html>"
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
                        "connector-source",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            operator_token_path = Path(tmp) / "operator-token.json"
            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/connectors", headers={"Authorization": f"Bearer {reviewer_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["connector_count"], 1)
                self.assertEqual(payload["connectors"][0]["connector_id"], "url-connector-source")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/sync",
                    body=json.dumps({"connector_id": "url-connector-source"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["result"]["connector_id"], "url-connector-source")
                self.assertEqual(payload["result"]["synced_count"], 1)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/sync",
                    body=json.dumps({"connector_id": "url-connector-source"}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("connectors.sync", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_enqueue_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Queue Submission Workspace")
            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            operator_token_path = Path(tmp) / "operator-token.json"
            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/lint",
                    body=json.dumps({}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "lint")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/research",
                    body=json.dumps(
                        {
                            "question": "how do agent loops use memory?",
                            "limit": 3,
                            "mode": "memo",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "research")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/jobs", headers={"Authorization": f"Bearer {operator_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["queued_count"], 2)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/lint",
                    body=json.dumps({}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("jobs.run", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_enqueue_research_step_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Research Step Queue Workspace")
            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory keeps intermediate findings durable.\n",
                encoding="utf-8",
            )
            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--job-profile",
                        "literature-review",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            operator_token_path = Path(tmp) / "operator-token.json"
            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/research-step",
                    body=json.dumps(
                        {
                            "run": "latest",
                            "step_id": "build-paper-matrix",
                            "profile_name": "alpha",
                            "route_source": "cli_override",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "research_step")
                self.assertEqual(payload["job"]["worker_capability"], "research")
                self.assertEqual(payload["job"]["parameters"]["step_id"], "build-paper-matrix")
                self.assertEqual(payload["job"]["parameters"]["assignment_id"], "assignment-build-paper-matrix")
                self.assertEqual(payload["job"]["parameters"]["planned_review_roles"], ["reviewer"])
                self.assertEqual(payload["job"]["parameters"]["route_source"], "cli_override")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/research-step",
                    body=json.dumps(
                        {
                            "run": "latest",
                            "step_id": "build-paper-matrix",
                            "profile_name": "alpha",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("jobs.run", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_job_endpoints_still_require_operator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Queue Role Workspace")
            self.assertEqual(main(["access", "grant", "reviewer-1", "reviewer", "--workspace", str(root)]), 0)

            reviewer_token_path = Path(tmp) / "reviewer-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.run",
                        "--output-file",
                        str(reviewer_token_path),
                    ]
                ),
                0,
            )
            reviewer_token = json.loads(reviewer_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/lint",
                    body=json.dumps({}),
                    headers={
                        "Authorization": f"Bearer {reviewer_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 403)
                self.assertIn("does not have permission", payload["error"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_enqueue_remote_ingest_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Remote Ingest Workspace")

            repo_dir = Path(tmp) / "repo-source"
            repo_dir.mkdir(parents=True, exist_ok=True)
            (repo_dir / "README.md").write_text("# Remote Sample\n\nTracked by ingest.\n", encoding="utf-8")
            subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "checkout", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed repo"], cwd=repo_dir, check=True, capture_output=True, text=True)

            page_one = Path(tmp) / "page-one.html"
            page_two = Path(tmp) / "page-two.html"
            page_one.write_text(
                "<html><head><title>Page One</title></head><body><p>First captured page.</p></body></html>",
                encoding="utf-8",
            )
            page_two.write_text(
                "<html><head><title>Page Two</title></head><body><p>Second captured page.</p></body></html>",
                encoding="utf-8",
            )
            sitemap = Path(tmp) / "sitemap.xml"
            sitemap.write_text(
                "\n".join(
                    [
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">",
                        f"  <url><loc>{page_one.resolve().as_uri()}</loc></url>",
                        f"  <url><loc>{page_two.resolve().as_uri()}</loc></url>",
                        "</urlset>",
                    ]
                ),
                encoding="utf-8",
            )

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/ingest-url",
                    body=json.dumps(
                        {
                            "url": "data:text/html;charset=utf-8,<html><head><title>Remote Notes</title></head><body><p>Remote ingest body.</p></body></html>",
                            "name": "remote-notes",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "ingest_url")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/ingest-repo",
                    body=json.dumps({"source": repo_dir.resolve().as_uri(), "name": "remote-sample"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "ingest_repo")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/ingest-sitemap",
                    body=json.dumps({"source": sitemap.resolve().as_uri()}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "ingest_sitemap")
                connection.close()

                self.assertEqual(
                    main(
                        [
                            "worker",
                            "remote",
                            "--server-url",
                            f"http://{host}:{port}",
                            "--token",
                            operator_token,
                            "--worker-id",
                            "remote-ingest",
                            "--max-jobs",
                            "3",
                        ]
                    ),
                    0,
                )

                self.assertTrue((root / "raw" / "urls" / "remote-notes.md").exists())
                self.assertTrue((root / "raw" / "repos" / "remote-sample.md").exists())
                self.assertTrue((root / "raw" / "urls" / "page-one.md").exists())
                self.assertTrue((root / "raw" / "urls" / "page-two.md").exists())

                queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                self.assertEqual(queue_payload["queued_count"], 0)
                self.assertEqual(queue_payload["status_counts"]["completed"], 3)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_preview_artifacts_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Artifact Preview Workspace")
            artifact_path = root / "outputs" / "reports" / "artifact.md"
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text("# Artifact\n\nPreview me remotely.\n", encoding="utf-8")

            token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(token_path),
                    ]
                ),
                0,
            )
            token = json.loads(token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "GET",
                    "/api/artifacts/preview?path=outputs/reports/artifact.md",
                    headers={"Authorization": f"Bearer {token}"},
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["artifact"]["path"], "outputs/reports/artifact.md")
                self.assertEqual(payload["artifact"]["kind"], "text")
                self.assertIn("Preview me remotely.", payload["artifact"]["excerpt"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_export_and_import_sync_bundles_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp) / "source"
            target_root = Path(tmp) / "target"
            source = Workspace(source_root)
            target = Workspace(target_root)
            source.initialize(name="Hosted Sync Source")
            target.initialize(name="Hosted Sync Target")
            (source_root / "raw").mkdir(parents=True, exist_ok=True)
            (source_root / "raw" / "notes.md").write_text("# Notes\n\nPortable over HTTP.\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://control.source.test/api",
                        "--workspace",
                        str(source_root),
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
                        str(source_root),
                        "--capability",
                        "sync.import",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(source_root)]), 0)

            self.assertEqual(
                main(
                    [
                        "share",
                        "bind-control-plane",
                        "https://control.target.test/api",
                        "--workspace",
                        str(target_root),
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
                        str(target_root),
                        "--capability",
                        "sync.import",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(target_root)]), 0)
            self.assertEqual(
                main(
                    [
                        "share",
                        "set-policy",
                        "--workspace",
                        str(target_root),
                        "--allow-sync-imports",
                    ]
                ),
                0,
            )

            source_token_path = Path(tmp) / "source-token.json"
            target_token_path = Path(tmp) / "target-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(source_root),
                        "--output-file",
                        str(source_token_path),
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
                        str(target_root),
                        "--output-file",
                        str(target_token_path),
                    ]
                ),
                0,
            )
            source_token = json.loads(source_token_path.read_text(encoding="utf-8"))["token"]
            target_token = json.loads(target_token_path.read_text(encoding="utf-8"))["token"]

            source_server = create_control_plane_server(workspace=source, host="127.0.0.1", port=0)
            target_server = create_control_plane_server(workspace=target, host="127.0.0.1", port=0)
            source_thread = threading.Thread(target=source_server.serve_forever, daemon=True)
            target_thread = threading.Thread(target=target_server.serve_forever, daemon=True)
            source_thread.start()
            target_thread.start()
            try:
                source_host, source_port = source_server.server_address
                target_host, target_port = target_server.server_address

                connection = HTTPConnection(source_host, source_port, timeout=5)
                connection.request(
                    "POST",
                    "/api/sync/export",
                    body=json.dumps({"peer_ref": "remote-ops", "inline_archive": True}),
                    headers={
                        "Authorization": f"Bearer {source_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                export_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(export_payload["bundle"]["shared_peer"]["peer_id"], "remote-ops")
                self.assertTrue(export_payload["archive_base64"])
                connection.close()

                connection = HTTPConnection(target_host, target_port, timeout=5)
                connection.request(
                    "POST",
                    "/api/sync/import",
                    body=json.dumps(
                        {
                            "archive_base64": export_payload["archive_base64"],
                            "from_peer": "remote-ops",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {target_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                import_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertGreater(import_payload["sync_event"]["file_count"], 0)
                connection.close()

                self.assertTrue((target_root / "raw" / "notes.md").exists())
                self.assertIn("Portable over HTTP.", (target_root / "raw" / "notes.md").read_text(encoding="utf-8"))
            finally:
                source_server.shutdown()
                target_server.shutdown()
                source_server.server_close()
                target_server.server_close()
                source_thread.join(timeout=5)
                target_thread.join(timeout=5)

    def test_control_plane_can_enqueue_peer_scoped_sync_exports_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Sync Export Queue Workspace")
            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
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
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/enqueue/sync-export",
                    body=json.dumps({"peer_ref": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["job"]["job_type"], "sync_export")
                self.assertEqual(payload["job"]["parameters"]["peer_ref"], "remote-ops")
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_register_and_schedule_connectors_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Connector Admin Workspace")

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                connector_url = "data:text/html;charset=utf-8,<html><head><title>Connector Source</title></head><body><p>Remote sync.</p></body></html>"

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/add",
                    body=json.dumps({"kind": "url", "source": connector_url, "name": "connector-source"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["connector"]["connector_id"], "url-connector-source")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/subscribe",
                    body=json.dumps({"connector_id": "url-connector-source", "every_hours": 1}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["connector"]["subscription"]["enabled"])
                self.assertEqual(payload["connector"]["subscription"]["interval_hours"], 1)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/connectors/unsubscribe",
                    body=json.dumps({"connector_id": "url-connector-source"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertFalse(payload["connector"]["subscription"]["enabled"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_invite_accept_and_issue_peer_bundles_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Peer Admin Workspace")
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

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/invite-peer",
                    body=json.dumps(
                        {
                            "peer_id": "remote-ops",
                            "role": "operator",
                            "base_url": "https://remote.example.test/cognisync",
                            "capabilities": ["jobs.remote"],
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["peer"]["status"], "pending")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/accept-peer",
                    body=json.dumps({"peer_ref": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["peer"]["status"], "accepted")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/share/issue-peer-bundle",
                    body=json.dumps({"peer_ref": "remote-ops"}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["bundle"]["principal_id"], "remote-ops")
                self.assertTrue(payload["bundle"]["token"].startswith("cp_"))
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

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
            self.assertIn("jobs.claim", bundle_payload["scopes"])
            self.assertIn("jobs.heartbeat", bundle_payload["scopes"])
            self.assertIn("jobs.run", bundle_payload["scopes"])
            self.assertNotIn("control.admin", bundle_payload["scopes"])

            sharing_payload = json.loads((root / ".cognisync" / "shared-workspace.json").read_text(encoding="utf-8"))
            peer = sharing_payload["peers"][0]
            self.assertEqual(peer["peer_id"], "remote-ops")
            self.assertTrue(peer["last_bundle_issued_at"])
            self.assertTrue(peer["last_token_id"])

            control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
            remote_tokens = [item for item in control_payload["tokens"] if item["principal_id"] == "remote-ops"]
            self.assertEqual(len(remote_tokens), 1)
            self.assertEqual(remote_tokens[0]["token_id"], peer["last_token_id"])

    def test_share_issue_peer_bundle_rejects_scopes_outside_capabilities(self) -> None:
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
                        "--capability",
                        "jobs.remote",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["share", "accept-peer", "remote-ops", "--workspace", str(root)]), 0)

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
                        "--scope",
                        "control.admin",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("not permitted by peer capabilities", stderr.getvalue())

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

    def test_remote_worker_session_is_visible_while_polling_without_an_active_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Polling Session Workspace")

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
            token_value = json.loads(token_stdout.read_text(encoding="utf-8"))["token"]

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
                        worker_id="remote-session",
                        max_jobs=1,
                        poll_interval_seconds=0.1,
                        max_idle_polls=20,
                    )

                worker_thread = threading.Thread(target=_run_worker, daemon=True)
                worker_thread.start()
                time.sleep(0.25)

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/workers", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                workers_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                session_worker = next(item for item in workers_payload["workers"] if item["worker_id"] == "remote-session")
                self.assertEqual(session_worker["status"], "polling")
                self.assertEqual(session_worker["declared_capabilities"], [])
                self.assertTrue(session_worker["last_seen_at"])
                connection.close()

                self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
                worker_thread.join(timeout=5)
                self.assertFalse(worker_thread.is_alive(), "remote worker did not finish after polling")
                self.assertEqual(worker_result["result"].processed_count, 1)
                self.assertEqual(worker_result["result"].completed_count, 1)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_control_plane_can_release_workers_and_requeue_active_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Worker Recovery Workspace")

            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)

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
                        "jobs.claim",
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
                connection.request(
                    "POST",
                    "/api/jobs/claim-next",
                    body=json.dumps({"worker_id": "stalled-worker", "lease_seconds": 600}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                claim_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200, claim_payload)
                claimed_job_id = claim_payload["job_id"]
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/workers/release",
                    body=json.dumps(
                        {
                            "worker_id": "stalled-worker",
                            "reason": "operator_recovery",
                            "requeue_active_jobs": True,
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                release_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200, release_payload)
                self.assertEqual(release_payload["requeued_job_ids"], [claimed_job_id])
                self.assertEqual(release_payload["session"], None)
                connection.close()

                queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                requeued_job = next(job for job in queue_payload["jobs"] if job["job_id"] == claimed_job_id)
                self.assertEqual(requeued_job["status"], "queued")
                self.assertEqual(requeued_job["worker_id"], "")

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/jobs/claim-next",
                    body=json.dumps({"worker_id": "replacement-worker", "lease_seconds": 300}),
                    headers={
                        "Authorization": f"Bearer {token_value}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                reclaim_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(reclaim_payload["job_id"], claimed_job_id)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_can_route_jobs_by_declared_capability_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Capability Routed Remote Worker Workspace")
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nAgents can revisit prior notes.\n",
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

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "research",
                        "how should agent memory be structured",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                worker_result = run_remote_worker(
                    server_url=f"http://{host}:{port}",
                    token=token_value,
                    worker_id="remote-workspace",
                    max_jobs=1,
                    worker_capabilities=["workspace"],
                )

                self.assertEqual(worker_result.processed_count, 1)
                self.assertEqual(worker_result.completed_count, 1)

                queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                queued_jobs = [job for job in queue_payload["jobs"] if job["status"] == "queued"]
                self.assertEqual(len(queued_jobs), 1)
                self.assertEqual(queued_jobs[0]["job_type"], "research")
                completed_job = next(job for job in queue_payload["jobs"] if job["status"] == "completed")
                self.assertEqual(completed_job["job_type"], "lint")

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/workers", headers={"Authorization": f"Bearer {token_value}"})
                response = connection.getresponse()
                workers_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(workers_payload["workers"][0]["worker_id"], "remote-workspace")
                self.assertEqual(workers_payload["workers"][0]["declared_capabilities"], ["workspace"])
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_can_execute_jobs_in_a_mirrored_workspace_and_sync_results_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Mirrored Remote Worker Server Workspace")
            (server_root / "raw" / "seed.md").write_text(
                "# Seed\n\nThe server workspace starts with one seed note.\n",
                encoding="utf-8",
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Mirrored Remote Worker Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "ingest-url",
                        "data:text/html;charset=utf-8,<html><head><title>Remote Mirror</title></head><body><p>Remote mirror execution.</p></body></html>",
                        "--workspace",
                        str(server_root),
                        "--name",
                        "remote-mirror",
                    ]
                ),
                0,
            )

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
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
                worker_result = run_remote_worker(
                    server_url=f"http://{host}:{port}",
                    token=token_value,
                    worker_id="remote-mirror",
                    max_jobs=1,
                    worker_capabilities=["ingest"],
                    workspace_root=mirror_root,
                )

                self.assertEqual(worker_result.processed_count, 1)
                self.assertEqual(worker_result.completed_count, 1)
                self.assertTrue((mirror_root / "raw" / "urls" / "remote-mirror.md").exists())
                self.assertTrue((server_root / "raw" / "urls" / "remote-mirror.md").exists())

                queue_payload = json.loads((server_root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                completed_job = next(job for job in queue_payload["jobs"] if job["status"] == "completed")
                self.assertEqual(completed_job["job_type"], "ingest_url")

                sync_history = json.loads((server_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
                self.assertGreaterEqual(sync_history["operation_counts"].get("import", 0), 1)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_can_execute_hosted_research_step_jobs_in_a_mirrored_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Mirrored Hosted Research Step Workspace")
            (server_root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (server_root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory keeps intermediate findings durable.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["alpha"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(server_root),
                        "--job-profile",
                        "literature-review",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "research-step",
                        "dispatch",
                        "--workspace",
                        str(server_root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--hosted",
                    ]
                ),
                0,
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Mirrored Hosted Research Step Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
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
                worker_result = run_remote_worker(
                    server_url=f"http://{host}:{port}",
                    token=token_value,
                    worker_id="remote-research",
                    max_jobs=4,
                    worker_capabilities=["research"],
                    workspace_root=mirror_root,
                )

                self.assertEqual(worker_result.processed_count, 4)
                self.assertEqual(worker_result.completed_count, 4)

                run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
                manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
                checkpoints_path = workspace.root / manifest["checkpoints_path"]
                checkpoints_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
                steps = {item["step_id"]: item for item in checkpoints_payload["steps"]}
                self.assertEqual(steps["build-working-set"]["execution_status"], "completed")
                self.assertEqual(steps["build-paper-matrix"]["execution_status"], "completed")
                self.assertEqual(steps["capture-open-questions"]["execution_status"], "completed")
                self.assertEqual(steps["execute-profile"]["execution_status"], "completed")
                self.assertTrue((server_root / manifest["answer_path"]).exists())
                self.assertTrue((mirror_root / manifest["answer_path"]).exists())

                queue_payload = json.loads((server_root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
                completed_jobs = [job for job in queue_payload["jobs"] if job["job_type"] == "research_step"]
                self.assertEqual(len(completed_jobs), 4)
                self.assertTrue(all(job["status"] == "completed" for job in completed_jobs))

                dispatch_manifest_path = server_root / checkpoints_payload["dispatch_history"][-1]
                dispatch_manifest = json.loads(dispatch_manifest_path.read_text(encoding="utf-8"))
                self.assertEqual(dispatch_manifest["dispatch_mode"], "hosted")
                self.assertEqual(dispatch_manifest["status"], "completed")
                self.assertEqual(
                    dispatch_manifest["executed_steps"],
                    ["build-working-set", "build-paper-matrix", "capture-open-questions", "execute-profile"],
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_hosted_research_steps_can_be_reconciled_without_rerunning_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Hosted Research Reconciliation Workspace")
            (server_root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (server_root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory keeps intermediate findings durable.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["alpha"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(server_root),
                        "--job-profile",
                        "literature-review",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "research-step",
                        "dispatch",
                        "--workspace",
                        str(server_root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--hosted",
                    ]
                ),
                0,
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Hosted Research Reconciliation Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
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
                worker_result = run_remote_worker(
                    server_url=f"http://{host}:{port}",
                    token=token_value,
                    worker_id="remote-research",
                    max_jobs=4,
                    worker_capabilities=["research"],
                    workspace_root=mirror_root,
                )

                self.assertEqual(worker_result.processed_count, 4)
                self.assertEqual(worker_result.completed_count, 4)

                for step_id in [
                    "build-working-set",
                    "build-paper-matrix",
                    "capture-open-questions",
                    "execute-profile",
                ]:
                    self.assertEqual(
                        main(
                            [
                                "research-step",
                                "review",
                                "--workspace",
                                str(server_root),
                                "--run",
                                "latest",
                                "--step",
                                step_id,
                                "--status",
                                "approved",
                                "--reviewer",
                                "operator-1",
                            ]
                        ),
                        0,
                    )

                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = main(
                        [
                            "research",
                            "--workspace",
                            str(server_root),
                            "--resume",
                            "latest",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                self.assertIn("Resumed research run", stdout.getvalue())

                manifest = json.loads(sorted((workspace.state_dir / "runs").glob("research-*.json"))[-1].read_text(encoding="utf-8"))
                self.assertEqual(manifest["status"], "completed")
                self.assertEqual(manifest["resume_strategy"], "checkpoint_finalize")
                self.assertEqual(manifest["reconciled_from_step_id"], "execute-profile")
                self.assertEqual(manifest["reconciled_from_assignment_id"], "assignment-execute-profile")

                checkpoints_path = workspace.root / manifest["checkpoints_path"]
                checkpoints_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
                steps = {item["step_id"]: item for item in checkpoints_payload["steps"]}
                self.assertEqual(steps["validate-citations"]["status"], "completed")
                self.assertEqual(steps["file-answer"]["status"], "completed")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_session_tracks_running_mirrored_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Running Remote Worker Session Workspace")
            (server_root / "raw" / "seed.md").write_text(
                "# Seed\n\nRunning workers should stay visible.\n",
                encoding="utf-8",
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Running Remote Worker Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "ingest-url",
                        "data:text/html;charset=utf-8,<html><head><title>Running Mirror</title></head><body><p>Track the running worker.</p></body></html>",
                        "--workspace",
                        str(server_root),
                        "--name",
                        "running-worker",
                    ]
                ),
                0,
            )

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
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
                worker_result = {}
                started = threading.Event()
                release = threading.Event()

                def _slow_execute_job_payload(*args, **kwargs):
                    started.set()
                    self.assertTrue(release.wait(timeout=5), "timed out waiting to release the slow mirrored job")
                    from cognisync.jobs import execute_job_payload as real_execute_job_payload

                    return real_execute_job_payload(*args, **kwargs)

                def _run_worker() -> None:
                    worker_result["result"] = run_remote_worker(
                        server_url=f"http://{host}:{port}",
                        token=token_value,
                        worker_id="remote-running",
                        max_jobs=1,
                        worker_capabilities=["ingest"],
                        workspace_root=mirror_root,
                    )

                with mock.patch("cognisync.remote_worker.execute_job_payload", side_effect=_slow_execute_job_payload):
                    worker_thread = threading.Thread(target=_run_worker, daemon=True)
                    worker_thread.start()
                    self.assertTrue(started.wait(timeout=5), "remote worker never reached running state")

                    connection = HTTPConnection(host, port, timeout=5)
                    connection.request("GET", "/api/workers", headers={"Authorization": f"Bearer {token_value}"})
                    response = connection.getresponse()
                    workers_payload = json.loads(response.read().decode("utf-8"))
                    self.assertEqual(response.status, 200)
                    running_worker = next(item for item in workers_payload["workers"] if item["worker_id"] == "remote-running")
                    self.assertEqual(running_worker["status"], "running")
                    self.assertEqual(running_worker["current_job_type"], "ingest_url")
                    self.assertTrue(running_worker["current_job_id"])
                    self.assertEqual(running_worker["declared_capabilities"], ["ingest"])
                    self.assertEqual(running_worker["workspace_root"], str(mirror_root.resolve()))
                    connection.close()

                    release.set()
                    worker_thread.join(timeout=5)
                    self.assertFalse(worker_thread.is_alive(), "remote worker did not finish after the running-state check")

                self.assertEqual(worker_result["result"].processed_count, 1)
                self.assertEqual(worker_result["result"].completed_count, 1)
                self.assertTrue((server_root / "raw" / "urls" / "running-worker.md").exists())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_renews_mirrored_job_leases_while_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Mirrored Lease Renewal Workspace")
            (server_root / "raw" / "seed.md").write_text(
                "# Seed\n\nLong-running mirrored jobs should renew their lease.\n",
                encoding="utf-8",
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Mirrored Lease Renewal Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "ingest-url",
                        "data:text/html;charset=utf-8,<html><head><title>Lease Renewal</title></head><body><p>Renew while running.</p></body></html>",
                        "--workspace",
                        str(server_root),
                        "--name",
                        "lease-renewal",
                    ]
                ),
                0,
            )

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
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
                worker_result = {}
                started = threading.Event()
                release = threading.Event()
                post_urls = []
                real_post_json = remote_worker_module._post_json

                def _slow_execute_job_payload(*args, **kwargs):
                    started.set()
                    self.assertTrue(release.wait(timeout=5), "timed out waiting to release the slow mirrored job")
                    from cognisync.jobs import execute_job_payload as real_execute_job_payload

                    return real_execute_job_payload(*args, **kwargs)

                def _recording_post_json(*args, **kwargs):
                    post_urls.append(str(args[0]))
                    return real_post_json(*args, **kwargs)

                def _run_worker() -> None:
                    try:
                        worker_result["result"] = run_remote_worker(
                            server_url=f"http://{host}:{port}",
                            token=token_value,
                            worker_id="remote-renewer",
                            max_jobs=1,
                            lease_seconds=3,
                            worker_capabilities=["ingest"],
                            workspace_root=mirror_root,
                        )
                    except Exception as error:  # pragma: no cover - exercised through assertions
                        worker_result["error"] = str(error)

                with mock.patch("cognisync.remote_worker.execute_job_payload", side_effect=_slow_execute_job_payload), mock.patch(
                    "cognisync.remote_worker._post_json",
                    side_effect=_recording_post_json,
                ):
                    worker_thread = threading.Thread(target=_run_worker, daemon=True)
                    worker_thread.start()
                    self.assertTrue(started.wait(timeout=5), "remote worker never reached the mirrored job")

                    queue_path = server_root / ".cognisync" / "jobs" / "queue.json"
                    initial_payload = json.loads(queue_path.read_text(encoding="utf-8"))
                    claimed_job = next(job for job in initial_payload["jobs"] if job["worker_id"] == "remote-renewer")
                    initial_expiry = str(claimed_job["lease_expires_at"])
                    initial_heartbeat = str(claimed_job["last_heartbeat_at"])
                    self.assertTrue(initial_heartbeat)

                    renewed_job = None
                    last_seen_job = claimed_job
                    deadline = time.time() + 5
                    while time.time() < deadline:
                        current_payload = json.loads(queue_path.read_text(encoding="utf-8"))
                        current_job = next(job for job in current_payload["jobs"] if job["job_id"] == claimed_job["job_id"])
                        last_seen_job = current_job
                        if (
                            current_job["last_heartbeat_at"] != initial_heartbeat
                            or current_job["lease_expires_at"] != initial_expiry
                        ):
                            renewed_job = current_job
                            break
                        time.sleep(0.1)

                    release.set()
                    worker_thread.join(timeout=5)
                    self.assertFalse(worker_thread.is_alive(), "remote worker did not finish after lease renewal")
                    self.assertNotIn("error", worker_result, worker_result.get("error"))
                    heartbeat_calls = [url for url in post_urls if url.endswith("/api/jobs/heartbeat")]
                    self.assertGreaterEqual(
                        len(heartbeat_calls),
                        2,
                        f"expected multiple heartbeat calls while mirrored work was running, got: {heartbeat_calls}",
                    )
                    self.assertIsNotNone(
                        renewed_job,
                        (
                            "mirrored worker never renewed its active lease while running: "
                            f"{last_seen_job}; heartbeat_calls={heartbeat_calls}"
                        ),
                    )
                    self.assertEqual(renewed_job["worker_id"], "remote-renewer")
                    self.assertNotEqual(renewed_job["lease_expires_at"], initial_expiry)

                self.assertEqual(worker_result["result"].processed_count, 1)
                self.assertEqual(worker_result["result"].completed_count, 1)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_remote_worker_can_refresh_mirrored_workspace_before_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server_root = Path(tmp) / "server-workspace"
            mirror_root = Path(tmp) / "mirror-workspace"
            workspace = Workspace(server_root)
            workspace.initialize(name="Refreshable Remote Worker Server Workspace")
            (server_root / "raw" / "seed.md").write_text(
                "# Seed\n\nThe initial mirror receives only this seed note.\n",
                encoding="utf-8",
            )

            initial_bundle = export_sync_bundle(workspace)
            mirror_workspace = Workspace(mirror_root)
            mirror_workspace.initialize(name="Refreshable Remote Worker Mirror")
            import_sync_bundle_archive(
                mirror_workspace,
                encode_sync_bundle_archive(initial_bundle.directory),
            )

            (server_root / "raw" / "fresh-server-note.md").write_text(
                "# Fresh Server Note\n\nThis note appeared on the hosted workspace after the mirror was created.\n",
                encoding="utf-8",
            )
            self.assertFalse((mirror_root / "raw" / "fresh-server-note.md").exists())
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(server_root)]), 0)

            token_stdout = Path(tmp) / "token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(server_root),
                        "--scope",
                        "control.read",
                        "--scope",
                        "jobs.claim",
                        "--scope",
                        "jobs.heartbeat",
                        "--scope",
                        "jobs.run",
                        "--scope",
                        "sync.export",
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
                worker_result = run_remote_worker(
                    server_url=f"http://{host}:{port}",
                    token=token_value,
                    worker_id="remote-refresh",
                    max_jobs=1,
                    worker_capabilities=["workspace"],
                    workspace_root=mirror_root,
                    refresh_workspace_before_jobs=True,
                )

                self.assertEqual(worker_result.processed_count, 1)
                self.assertEqual(worker_result.completed_count, 1)
                self.assertEqual(
                    (mirror_root / "raw" / "fresh-server-note.md").read_text(encoding="utf-8"),
                    (server_root / "raw" / "fresh-server-note.md").read_text(encoding="utf-8"),
                )
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

    def test_scheduler_can_enqueue_due_scheduled_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Scheduled Jobs Workspace")
            (root / "raw" / "memory.md").write_text("# Memory\n\nAgents keep notes.\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "schedule-research",
                        "map the open questions in memory systems",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "1",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "schedule-maintain",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "1",
                        "--max-concepts",
                        "2",
                        "--max-backlinks",
                        "2",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "scheduler-tick",
                        "--workspace",
                        str(root),
                        "--enqueue-only",
                    ]
                ),
                0,
            )

            queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
            queued_job_types = [job["job_type"] for job in queue_payload["jobs"]]
            self.assertEqual(queue_payload["queued_count"], 2)
            self.assertIn("research", queued_job_types)
            self.assertIn("maintain", queued_job_types)

            control_payload = json.loads((root / ".cognisync" / "control-plane.json").read_text(encoding="utf-8"))
            scheduler = control_payload["scheduler"]
            self.assertEqual(scheduler["last_action"], "enqueued")
            self.assertEqual(len(scheduler["last_due_job_subscription_ids"]), 2)

    def test_control_plane_can_manage_scheduled_jobs_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Scheduled Jobs Workspace")

            operator_token_path = Path(tmp) / "operator-token.json"
            self.assertEqual(
                main(
                    [
                        "control-plane",
                        "issue-token",
                        "local-operator",
                        "--workspace",
                        str(root),
                        "--output-file",
                        str(operator_token_path),
                    ]
                ),
                0,
            )
            operator_token = json.loads(operator_token_path.read_text(encoding="utf-8"))["token"]

            server = create_control_plane_server(workspace=workspace, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/scheduler/jobs/research",
                    body=json.dumps(
                        {
                            "question": "track contradictions in deployment notes",
                            "every_hours": 1,
                            "mode": "memo",
                        }
                    ),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                subscription_id = payload["subscription"]["subscription_id"]
                self.assertEqual(payload["subscription"]["job_type"], "research")
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request("GET", "/api/scheduler/jobs", headers={"Authorization": f"Bearer {operator_token}"})
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["summary"]["subscription_count"], 1)
                self.assertEqual(payload["items"][0]["subscription_id"], subscription_id)
                connection.close()

                connection = HTTPConnection(host, port, timeout=5)
                connection.request(
                    "POST",
                    "/api/scheduler/jobs/remove",
                    body=json.dumps({"subscription_id": subscription_id}),
                    headers={
                        "Authorization": f"Bearer {operator_token}",
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["subscription"]["enabled"], False)
                connection.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
