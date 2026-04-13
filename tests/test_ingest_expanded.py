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
from cognisync.workspace import Workspace


class ExpandedIngestTests(unittest.TestCase):
    def test_ingest_notebook_writes_sidecar_and_copies_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Notebook Ingest Test")

            notebook = root / "memory-analysis.ipynb"
            notebook.write_text(
                json.dumps(
                    {
                        "cells": [
                            {
                                "cell_type": "markdown",
                                "metadata": {},
                                "source": ["# Memory Analysis\n", "\n", "Notebook notes about durable recall."],
                            },
                            {
                                "cell_type": "code",
                                "execution_count": 1,
                                "metadata": {},
                                "source": ["print('memory retrieval')\n"],
                                "outputs": [
                                    {"output_type": "stream", "name": "stdout", "text": ["memory retrieval\n"]},
                                    {"output_type": "display_data", "data": {"image/png": "iVBORw0KGgo="}, "metadata": {}},
                                ],
                            },
                        ],
                        "metadata": {
                            "kernelspec": {"display_name": "Python 3", "name": "python3"},
                            "language_info": {"name": "python"},
                        },
                        "nbformat": 4,
                        "nbformat_minor": 5,
                    }
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "ingest",
                        "notebook",
                        str(notebook),
                        "--workspace",
                        str(workspace.root),
                        "--name",
                        "memory-analysis",
                    ]
                )

            self.assertEqual(exit_code, 0)
            copied_notebook = workspace.raw_dir / "notebooks" / "memory-analysis.ipynb"
            sidecar = workspace.raw_dir / "notebooks" / "memory-analysis.md"
            self.assertTrue(copied_notebook.exists())
            self.assertTrue(sidecar.exists())
            sidecar_text = sidecar.read_text(encoding="utf-8")
            self.assertIn("tags: [notebook-ingest]", sidecar_text)
            self.assertIn("- Markdown cells: `1`", sidecar_text)
            self.assertIn("- Code cells: `1`", sidecar_text)
            self.assertIn("- Kernel: `Python 3`", sidecar_text)
            self.assertIn("# Memory Analysis", sidecar_text)
            self.assertIn("```python", sidecar_text)
            self.assertIn("print('memory retrieval')", sidecar_text)
            self.assertIn("- Output count: `2`", sidecar_text)
            self.assertIn("display_data", sidecar_text)
            self.assertIn("Ingested notebook into", stdout.getvalue())

    def test_ingest_dataset_descriptors_support_json_and_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Dataset Ingest Test")

            descriptor = root / "dataset-card.json"
            descriptor.write_text(
                json.dumps(
                    {
                        "name": "memory-corpus",
                        "license": "MIT",
                        "features": ["question", "answer"],
                        "splits": {"train": 12, "validation": 3},
                    }
                ),
                encoding="utf-8",
            )
            table = root / "examples.csv"
            table.write_text("question,answer\nWhat is memory?,Durable recall\nWhat is retrieval?,Search\n", encoding="utf-8")

            self.assertEqual(
                main(["ingest", "dataset", str(descriptor), "--workspace", str(workspace.root), "--name", "memory-corpus"]),
                0,
            )
            self.assertEqual(
                main(["ingest", "dataset", str(table), "--workspace", str(workspace.root), "--name", "memory-examples"]),
                0,
            )

            json_sidecar = workspace.raw_dir / "datasets" / "memory-corpus.md"
            csv_sidecar = workspace.raw_dir / "datasets" / "memory-examples.md"
            self.assertTrue((workspace.raw_dir / "datasets" / "memory-corpus.json").exists())
            self.assertTrue((workspace.raw_dir / "datasets" / "memory-examples.csv").exists())
            self.assertTrue(json_sidecar.exists())
            self.assertTrue(csv_sidecar.exists())

            json_text = json_sidecar.read_text(encoding="utf-8")
            self.assertIn("tags: [dataset-ingest]", json_text)
            self.assertIn("- Descriptor type: `json`", json_text)
            self.assertIn("- `features`", json_text)
            self.assertIn("- `splits`", json_text)
            self.assertIn("descriptor-level ingest", json_text)

            csv_text = csv_sidecar.read_text(encoding="utf-8")
            self.assertIn("- Descriptor type: `csv`", csv_text)
            self.assertIn("- Column count: `2`", csv_text)
            self.assertIn("- Rows previewed: `2`", csv_text)
            self.assertIn("| question | answer |", csv_text)

    def test_ingest_image_folder_copies_assets_and_renders_captions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Image Folder Ingest Test")

            image_dir = root / "diagrams"
            image_dir.mkdir()
            (image_dir / "overview.png").write_bytes(b"\x89PNG\r\n\x1a\n")
            (image_dir / "pipeline.svg").write_text("<svg><title>Pipeline</title></svg>", encoding="utf-8")
            (image_dir / "overview.md").write_text("Overview architecture caption.", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "ingest",
                        "image-folder",
                        str(image_dir),
                        "--workspace",
                        str(workspace.root),
                        "--name",
                        "architecture-sketches",
                    ]
                )

            self.assertEqual(exit_code, 0)
            sidecar = workspace.raw_dir / "images" / "architecture-sketches.md"
            asset_dir = workspace.raw_dir / "images" / "architecture-sketches-assets"
            self.assertTrue(sidecar.exists())
            self.assertTrue((asset_dir / "overview.png").exists())
            self.assertTrue((asset_dir / "pipeline.svg").exists())
            sidecar_text = sidecar.read_text(encoding="utf-8")
            self.assertIn("tags: [image-folder-ingest]", sidecar_text)
            self.assertIn("- Image count: `2`", sidecar_text)
            self.assertIn("![overview.png](architecture-sketches-assets/overview.png)", sidecar_text)
            self.assertIn("Overview architecture caption.", sidecar_text)
            self.assertIn("Ingested image folder into", stdout.getvalue())

    def test_ingest_batch_supports_notebooks_datasets_and_image_folders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="New Ingest Batch Test")

            notebook = root / "analysis.ipynb"
            notebook.write_text(
                json.dumps(
                    {
                        "cells": [{"cell_type": "markdown", "metadata": {}, "source": ["# Analysis"]}],
                        "metadata": {},
                        "nbformat": 4,
                        "nbformat_minor": 5,
                    }
                ),
                encoding="utf-8",
            )
            dataset = root / "dataset.json"
            dataset.write_text('{"name": "batch-dataset", "features": ["text"]}', encoding="utf-8")
            images = root / "images"
            images.mkdir()
            (images / "chart.png").write_bytes(b"\x89PNG\r\n\x1a\n")

            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "items": [
                            {"kind": "notebook", "source": str(notebook), "name": "batch-notebook"},
                            {"kind": "dataset", "source": str(dataset), "name": "batch-dataset"},
                            {"kind": "image-folder", "source": str(images), "name": "batch-images"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["ingest", "batch", str(manifest), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.raw_dir / "notebooks" / "batch-notebook.md").exists())
            self.assertTrue((workspace.raw_dir / "datasets" / "batch-dataset.md").exists())
            self.assertTrue((workspace.raw_dir / "images" / "batch-images.md").exists())
            self.assertIn("Batch ingested 3 source(s).", stdout.getvalue())

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
