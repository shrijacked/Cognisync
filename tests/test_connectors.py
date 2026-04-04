import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main


class ConnectorTests(unittest.TestCase):
    def test_connector_sync_all_runs_every_registered_connector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Sync All Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>First Source</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Second Source</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "first-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "second-source",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                sync_exit = main(["connector", "sync-all", "--workspace", str(root)])
            self.assertEqual(sync_exit, 0)
            self.assertIn("Synced 2 connector(s).", stdout.getvalue())
            self.assertIn("Imported 2 source artifact(s).", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "first-source.md").exists())
            self.assertTrue((root / "raw" / "urls" / "second-source.md").exists())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connectors = sorted(registry["connectors"], key=lambda item: item["connector_id"])
            self.assertEqual(len(connectors), 2)
            self.assertTrue(all(connector["last_synced_at"] for connector in connectors))
            self.assertEqual(sum(int(connector["last_result_count"]) for connector in connectors), 2)

    def test_connector_add_list_and_sync_repo_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Workspace"]), 0)

            repo_dir = Path(tmp) / "sample-repo"
            repo_dir.mkdir()
            (repo_dir / "README.md").write_text("# Sample Repo\n\nConnector sync.\n", encoding="utf-8")
            subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "checkout", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed repo"], cwd=repo_dir, check=True, capture_output=True, text=True)

            remote_source = repo_dir.resolve().as_uri()
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "repo",
                        remote_source,
                        "--workspace",
                        str(root),
                        "--name",
                        "remote-sample",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                list_exit = main(["connector", "list", "--workspace", str(root)])
            self.assertEqual(list_exit, 0)
            self.assertIn("repo-remote-sample", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                sync_exit = main(["connector", "sync", "repo-remote-sample", "--workspace", str(root)])
            self.assertEqual(sync_exit, 0)
            self.assertIn("Synced connector repo-remote-sample", stdout.getvalue())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connector = registry["connectors"][0]
            self.assertEqual(connector["connector_id"], "repo-remote-sample")
            self.assertTrue(connector["last_synced_at"])
            self.assertEqual(connector["last_result_count"], 1)
            self.assertTrue((root / "raw" / "repos" / "remote-sample.md").exists())

    def test_connector_sync_jobs_flow_through_the_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Job Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>First Source</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Second Source</title></head><body><p>Two.</p></body></html>")
            url_list = Path(tmp) / "urls.txt"
            url_list.write_text(first_url + "\n" + second_url + "\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "urls",
                        str(url_list),
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-sources",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "connector-sync",
                        "urls-batch-sources",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "1"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 1 job(s): 1 completed, 0 failed.", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "first-source.md").exists())
            self.assertTrue((root / "raw" / "urls" / "second-source.md").exists())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connector = registry["connectors"][0]
            self.assertEqual(connector["connector_id"], "urls-batch-sources")
            self.assertEqual(connector["last_result_count"], 2)

            manifest_payloads = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            ]
            self.assertEqual(manifest_payloads[0]["job_type"], "connector_sync")
            self.assertEqual(manifest_payloads[0]["result"]["connector_kind"], "urls")

    def test_connector_sync_all_jobs_flow_through_the_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Batch Job Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Batch First</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Batch Second</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-first",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-second",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "connector-sync-all",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "1"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 1 job(s): 1 completed, 0 failed.", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "batch-first.md").exists())
            self.assertTrue((root / "raw" / "urls" / "batch-second.md").exists())

            manifest_payloads = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            ]
            self.assertEqual(manifest_payloads[0]["job_type"], "connector_sync_all")
            self.assertEqual(manifest_payloads[0]["result"]["synced_connector_count"], 2)
            self.assertEqual(manifest_payloads[0]["result"]["total_result_count"], 2)


if __name__ == "__main__":
    unittest.main()
