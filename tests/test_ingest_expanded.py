import io
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.workspace import Workspace


class ExpandedIngestTests(unittest.TestCase):
    def test_ingest_repo_can_clone_a_remote_git_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Remote Repo Ingest Test")

            repo_dir = root / "sample-repo"
            repo_dir.mkdir()
            (repo_dir / "README.md").write_text("# Sample Repo\n\nRemote repo ingest.\n", encoding="utf-8")
            (repo_dir / "cli.py").write_text("print('ok')\n", encoding="utf-8")
            subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "checkout", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed repo"], cwd=repo_dir, check=True, capture_output=True, text=True)

            remote_source = repo_dir.resolve().as_uri()
            exit_code = main(["ingest", "repo", remote_source, "--workspace", str(workspace.root), "--name", "remote-sample"])

            self.assertEqual(exit_code, 0)
            manifest_path = workspace.raw_dir / "repos" / "remote-sample.md"
            self.assertTrue(manifest_path.exists())
            text = manifest_path.read_text(encoding="utf-8")
            self.assertIn("Source repo:", text)
            self.assertIn(remote_source, text)
            self.assertIn("seed repo", text)

    def test_ingest_urls_reads_a_list_of_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="URL List Ingest Test")

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>First Url</title></head><body><p>First body.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Second Url</title></head><body><p>Second body.</p></body></html>")
            url_list = root / "urls.txt"
            url_list.write_text(first_url + "\n" + second_url + "\n", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["ingest", "urls", str(url_list), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.raw_dir / "urls" / "first-url.md").exists())
            self.assertTrue((workspace.raw_dir / "urls" / "second-url.md").exists())
            self.assertIn("Ingested 2 URL source(s).", stdout.getvalue())

    def test_ingest_sitemap_fetches_all_urls_in_the_document(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Sitemap Ingest Test")

            page_one = root / "page-one.html"
            page_two = root / "page-two.html"
            page_one.write_text("<html><head><title>Page One</title></head><body><p>One.</p></body></html>", encoding="utf-8")
            page_two.write_text("<html><head><title>Page Two</title></head><body><p>Two.</p></body></html>", encoding="utf-8")

            sitemap = root / "sitemap.xml"
            sitemap.write_text(
                "\n".join(
                    [
                        '<?xml version="1.0" encoding="UTF-8"?>',
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
                        f"  <url><loc>{page_one.resolve().as_uri()}</loc></url>",
                        f"  <url><loc>{page_two.resolve().as_uri()}</loc></url>",
                        "</urlset>",
                    ]
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["ingest", "sitemap", str(sitemap), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.raw_dir / "urls" / "page-one.md").exists())
            self.assertTrue((workspace.raw_dir / "urls" / "page-two.md").exists())
            self.assertIn("Ingested 2 URL source(s).", stdout.getvalue())

    def test_ingest_batch_supports_url_lists_and_sitemaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Expanded Batch Ingest Test")

            page = root / "batch-page.html"
            page.write_text("<html><head><title>Batch Page</title></head><body><p>Batch body.</p></body></html>", encoding="utf-8")
            sitemap = root / "sitemap.xml"
            sitemap.write_text(
                "\n".join(
                    [
                        '<?xml version="1.0" encoding="UTF-8"?>',
                        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
                        f"  <url><loc>{page.resolve().as_uri()}</loc></url>",
                        "</urlset>",
                    ]
                ),
                encoding="utf-8",
            )

            list_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Batch List</title></head><body><p>List body.</p></body></html>")
            url_list = root / "urls.txt"
            url_list.write_text(list_url + "\n", encoding="utf-8")

            manifest = root / "manifest.json"
            manifest.write_text(
                "{\n"
                '  "items": [\n'
                f'    {{"kind": "urls", "source": "{url_list}"}},\n'
                f'    {{"kind": "sitemap", "source": "{sitemap}"}}\n'
                "  ]\n"
                "}\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["ingest", "batch", str(manifest), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.raw_dir / "urls" / "batch-list.md").exists())
            self.assertTrue((workspace.raw_dir / "urls" / "batch-page.md").exists())
            self.assertIn("Batch ingested 2 source(s).", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
