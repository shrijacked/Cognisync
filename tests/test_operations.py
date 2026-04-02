import io
import base64
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.planner import build_compile_plan
from cognisync.scanner import scan_workspace
from cognisync.config import LLMProfile, load_config, save_config
from cognisync.workspace import Workspace


def _build_test_pdf_bytes(text: str) -> bytes:
    stream = f"BT\n/F1 24 Tf\n72 100 Td\n({text}) Tj\nET\n".encode("utf-8")
    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        (
            b"3 0 obj\n"
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] "
            b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\n"
            b"endobj\n"
        ),
        b"4 0 obj\n<< /Length %d >>\nstream\n%bendstream\nendobj\n" % (len(stream), stream),
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    ]
    header = b"%PDF-1.4\n"
    body = bytearray(header)
    offsets = [0]
    for obj in objects:
        offsets.append(len(body))
        body.extend(obj)
    startxref = len(body)
    body.extend(f"xref\n0 {len(offsets)}\n".encode("utf-8"))
    body.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        body.extend(f"{offset:010d} 00000 n \n".encode("utf-8"))
    body.extend(
        (
            f"trailer\n<< /Root 1 0 R /Size {len(offsets)} >>\n"
            f"startxref\n{startxref}\n%%EOF\n"
        ).encode("utf-8")
    )
    return bytes(body)


class OperationsTests(unittest.TestCase):
    def test_doctor_reports_clean_workspace_with_configured_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Doctor Test")

            config = load_config(workspace.config_path)
            config.llm_profiles["python"] = LLMProfile(command=[sys.executable, "-c", "print('ok')"])
            save_config(workspace.config_path, config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["doctor", "--workspace", str(root), "--strict"])

            self.assertEqual(exit_code, 0)
            self.assertIn("PASS", stdout.getvalue())
            self.assertIn("workspace_config", stdout.getvalue())

    def test_doctor_flags_missing_adapter_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Doctor Fail Test")

            config = load_config(workspace.config_path)
            config.llm_profiles["missing"] = LLMProfile(command=["definitely-not-a-real-command-12345"])
            save_config(workspace.config_path, config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["doctor", "--workspace", str(root), "--strict"])

            self.assertEqual(exit_code, 1)
            self.assertIn("FAIL", stdout.getvalue())
            self.assertIn("profile:missing", stdout.getvalue())

    def test_ingest_file_and_pdf_copy_assets_into_raw(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = Path(tmp) / "source-assets"
            source_root.mkdir()
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Ingest Test")

            note = source_root / "note.md"
            note.write_text("# Note\n\nIngest me.\n", encoding="utf-8")
            pdf = source_root / "paper.pdf"
            pdf.write_bytes(_build_test_pdf_bytes("copy asset pdf"))

            self.assertEqual(main(["ingest", "file", str(note), "--workspace", str(workspace.root)]), 0)
            self.assertEqual(main(["ingest", "pdf", str(pdf), "--workspace", str(workspace.root)]), 0)

            self.assertTrue((workspace.root / "raw" / "files" / "note.md").exists())
            self.assertTrue((workspace.root / "raw" / "pdfs" / "paper.pdf").exists())

    def test_ingest_pdf_extracts_text_into_a_sidecar_markdown_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = Path(tmp) / "source-assets"
            source_root.mkdir()
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="PDF Ingest Test")

            pdf = source_root / "paper.pdf"
            pdf.write_bytes(_build_test_pdf_bytes("agent memory loops"))

            exit_code = main(["ingest", "pdf", str(pdf), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            copied_pdf = workspace.root / "raw" / "pdfs" / "paper.pdf"
            sidecar = workspace.root / "raw" / "pdfs" / "paper.md"
            self.assertTrue(copied_pdf.exists())
            self.assertTrue(sidecar.exists())
            text = sidecar.read_text(encoding="utf-8")
            self.assertIn("tags: [pdf-ingest]", text)
            self.assertIn("source_file: paper.pdf", text)
            self.assertIn("## Extracted Metadata", text)
            self.assertIn("## Extracted Text", text)
            self.assertIn("agent memory loops", text)

    def test_ingest_pdf_sidecar_does_not_duplicate_summary_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = Path(tmp) / "source-assets"
            source_root.mkdir()
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="PDF Plan Test")

            pdf = source_root / "paper.pdf"
            pdf.write_bytes(_build_test_pdf_bytes("agent memory loops"))

            self.assertEqual(main(["ingest", "pdf", str(pdf), "--workspace", str(workspace.root)]), 0)

            snapshot = scan_workspace(workspace)
            plan = build_compile_plan(snapshot)
            pdf_summary_tasks = [task for task in plan.tasks if task.output_path == "wiki/sources/paper.md"]

            self.assertEqual(len(pdf_summary_tasks), 1)

    def test_ingest_url_fetches_html_and_converts_it_to_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="URL Ingest Test")

            image_bytes = base64.b64encode(
                bytes.fromhex(
                    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
                    "0000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082"
                )
            ).decode("ascii")
            html = (
                "<html><head><title>Edge Agents</title>"
                '<meta name="description" content="Local agent systems for edge inference.">'
                '<link rel="canonical" href="https://example.com/edge-agents">'
                "</head><body><h1>Edge Agents</h1><h2>Deployment</h2>"
                f'<img alt="system diagram" src="data:image/png;base64,{image_bytes}">'
                '<p>Data url ingest test with <a href="https://example.com/docs">docs</a>.</p>'
                "</body></html>"
            )
            url = "data:text/html;charset=utf-8," + quote(html)
            exit_code = main(["ingest", "url", url, "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            files = list((workspace.root / "raw" / "urls").glob("*.md"))
            self.assertEqual(len(files), 1)
            text = files[0].read_text(encoding="utf-8")
            self.assertIn("# Edge Agents", text)
            self.assertIn("Source URL:", text)
            self.assertIn("description: Local agent systems for edge inference.", text)
            self.assertIn("canonical_url: https://example.com/edge-agents", text)
            self.assertIn("## Extracted Metadata", text)
            self.assertIn("- Heading count: `2`", text)
            self.assertIn("- Outbound link count: `1`", text)
            self.assertIn("- Captured image count: `1`", text)
            self.assertIn("## Discovered Links", text)
            self.assertIn("https://example.com/docs", text)
            self.assertIn("## Captured Images", text)
            self.assertIn("system-diagram-1.png", text)

            image_files = list((workspace.root / "raw" / "urls" / "edge-agents-assets").glob("*.png"))
            self.assertEqual(len(image_files), 1)

    def test_ingest_repo_creates_manifest_for_local_repository(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Repo Ingest Test")

            repo_dir = root / "sample-repo"
            repo_dir.mkdir()
            (repo_dir / "README.md").write_text("# Sample Repo\n\nAgent systems.\n", encoding="utf-8")
            (repo_dir / "main.py").write_text("print('hello')\n", encoding="utf-8")
            (repo_dir / "helper.js").write_text("console.log('hi')\n", encoding="utf-8")
            nested_dir = repo_dir / "src" / "agents"
            nested_dir.mkdir(parents=True)
            (nested_dir / "planner.py").write_text("def run():\n    return 'ok'\n", encoding="utf-8")

            subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "checkout", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "commit", "-m", "seed repo"],
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            exit_code = main(["ingest", "repo", str(repo_dir), "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            manifest_files = list((workspace.root / "raw" / "repos").glob("*.md"))
            self.assertEqual(len(manifest_files), 1)
            text = manifest_files[0].read_text(encoding="utf-8")
            self.assertIn("# sample-repo", text)
            self.assertIn("Current branch: `main`", text)
            self.assertIn("README excerpt", text)
            self.assertIn("## Repository Stats", text)
            self.assertIn("- File count: `4`", text)
            self.assertIn("## Language Signals", text)
            self.assertIn("- `python`: 2 file(s)", text)
            self.assertIn("- `javascript`: 1 file(s)", text)
            self.assertIn("## Recent Commits", text)
            self.assertIn("seed repo", text)
            self.assertIn("## Repository Tree Snapshot", text)
            self.assertIn("src/", text)
            self.assertIn("agents/", text)
            self.assertIn("planner.py", text)

    def test_compile_runs_profile_writes_summary_and_passes_lint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Compile Test")

            (workspace.root / "raw" / "research-note.md").write_text(
                "# Research Note\n\nAgent loops help keep work grounded.\n",
                encoding="utf-8",
            )

            script = textwrap.dedent(
                """
                import pathlib
                import sys

                prompt = sys.stdin.read()
                root = pathlib.Path.cwd()
                target = root / "wiki" / "sources" / "research-note.md"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(
                    "# Research Note\\n\\nGenerated by compile loop.\\n\\n- [[sources]]\\n",
                    encoding="utf-8",
                )
                index_page = root / "wiki" / "sources.md"
                index_page.write_text(
                    "# Sources\\n\\n- [Research Note](sources/research-note.md)\\n",
                    encoding="utf-8",
                )
                print("compile finished")
                """
            ).strip()

            config = load_config(workspace.config_path)
            config.llm_profiles["compiler"] = LLMProfile(
                command=[sys.executable, "-c", script],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            output_path = workspace.root / "outputs" / "reports" / "compile-run.txt"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "compile",
                        "--workspace",
                        str(workspace.root),
                        "--profile",
                        "compiler",
                        "--output-file",
                        str(output_path),
                        "--strict",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.root / "wiki" / "sources" / "research-note.md").exists())
            self.assertTrue(output_path.exists())
            self.assertIn("compile finished", output_path.read_text(encoding="utf-8"))
            self.assertIn("Compile finished with no lint issues.", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
