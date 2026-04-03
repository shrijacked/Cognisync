import io
import base64
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
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


class RuntimeContractsTests(unittest.TestCase):
    def test_scan_writes_source_and_graph_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = root / "source-assets"
            source_root.mkdir()
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Manifest Test")

            (workspace.wiki_dir / "concepts" / "agent-loops.md").write_text(
                "# Agent Loops\n\nA concept page for agent loops.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "note.md").write_text(
                "# Note\n\nSee [[agent-loops]] for the compiled concept.\n",
                encoding="utf-8",
            )

            pdf = source_root / "paper.pdf"
            pdf.write_bytes(_build_test_pdf_bytes("memory retrieval paper"))
            self.assertEqual(main(["ingest", "pdf", str(pdf), "--workspace", str(workspace.root)]), 0)

            image_bytes = base64.b64encode(
                bytes.fromhex(
                    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
                    "0000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082"
                )
            ).decode("ascii")
            html = (
                "<html><head><title>Vision Capture</title></head><body>"
                f'<img alt="system overview" src="data:image/png;base64,{image_bytes}">'
                "<p>Image-aware ingest.</p>"
                "</body></html>"
            )
            url = "data:text/html;charset=utf-8," + quote(html)
            self.assertEqual(main(["ingest", "url", url, "--workspace", str(workspace.root)]), 0)
            self.assertEqual(main(["scan", "--workspace", str(workspace.root)]), 0)

            sources_manifest = workspace.state_dir / "sources.json"
            graph_manifest = workspace.state_dir / "graph.json"
            self.assertTrue(sources_manifest.exists())
            self.assertTrue(graph_manifest.exists())

            sources_payload = json.loads(sources_manifest.read_text(encoding="utf-8"))
            self.assertEqual(sources_payload["schema_version"], 1)
            pdf_record = next(source for source in sources_payload["sources"] if source["source_kind"] == "pdf")
            self.assertIn("raw/pdfs/paper.pdf", pdf_record["artifacts"])
            self.assertIn("raw/pdfs/paper.md", pdf_record["artifacts"])
            self.assertEqual(pdf_record["extraction_status"], "text_available")

            url_record = next(source for source in sources_payload["sources"] if source["source_kind"] == "url")
            self.assertTrue(url_record["captured_assets"])

            graph_payload = json.loads(graph_manifest.read_text(encoding="utf-8"))
            artifact_nodes = {node["path"] for node in graph_payload["nodes"] if node["kind"] == "artifact"}
            self.assertIn("raw/note.md", artifact_nodes)
            self.assertIn("wiki/concepts/agent-loops.md", artifact_nodes)

            link_edges = [
                edge for edge in graph_payload["edges"] if edge["kind"] == "link" and edge["source"] == "raw/note.md"
            ]
            self.assertEqual(len(link_edges), 1)
            self.assertEqual(link_edges[0]["target"], "wiki/concepts/agent-loops.md")

    def test_search_engine_applies_source_type_aware_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Ranking Test")

            (workspace.raw_dir / "repos").mkdir(exist_ok=True)
            (workspace.raw_dir / "pdfs").mkdir(exist_ok=True)
            (workspace.raw_dir / "urls").mkdir(exist_ok=True)

            (workspace.raw_dir / "repos" / "agent-kit.md").write_text(
                "---\n"
                "title: Agent Kit\n"
                "tags: [repo-ingest]\n"
                "---\n"
                "# Agent Kit\n\n"
                "Repository module layout for the CLI implementation and API surface.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "pdfs" / "memory-study.md").write_text(
                "---\n"
                "title: Memory Study\n"
                "tags: [pdf-ingest]\n"
                "---\n"
                "# Memory Study\n\n"
                "Paper on retrieval, citations, and literature review methodology.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "urls" / "vision-capture.md").write_text(
                "---\n"
                "title: Vision Capture\n"
                "tags: [url-ingest]\n"
                "---\n"
                "# Vision Capture\n\n"
                "Architecture diagram and image-heavy walkthrough.\n\n"
                "![diagram](vision-assets/diagram.png)\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)
            engine = SearchEngine.from_workspace(workspace, snapshot)

            repo_hits = engine.search("which repository module implements the cli api", limit=3)
            pdf_hits = engine.search("which paper covers literature review and citations", limit=3)
            visual_hits = engine.search("show me the architecture diagram image", limit=3)

            self.assertEqual(repo_hits[0].path, "raw/repos/agent-kit.md")
            self.assertEqual(pdf_hits[0].path, "raw/pdfs/memory-study.md")
            self.assertEqual(visual_hits[0].path, "raw/urls/vision-capture.md")

    def test_research_mode_writes_run_manifest_and_mode_specific_answer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Mode Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory. #agents\n",
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

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
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
                )

            self.assertEqual(exit_code, 0)
            answer_path = workspace.outputs_dir / "reports" / "how-do-agent-loops-use-memory-memo.md"
            packet_path = workspace.prompts_dir / "query-how-do-agent-loops-use-memory.md"
            self.assertTrue(answer_path.exists())
            self.assertTrue(packet_path.exists())
            self.assertIn("research memo", packet_path.read_text(encoding="utf-8").lower())
            self.assertIn("Wrote filed answer", stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertTrue(run_manifests)
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertEqual(manifest["mode"], "memo")
            self.assertEqual(manifest["question"], "how do agent loops use memory")
            self.assertTrue(manifest["validation"]["passed"])
            self.assertEqual(manifest["citations"]["used"], ["S1"])
            self.assertEqual(manifest["answer_path"], "outputs/reports/how-do-agent-loops-use-memory-memo.md")
            self.assertEqual(manifest["status"], "completed")
            self.assertIn("plan_path", manifest)

    def test_research_without_profile_writes_a_resumable_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Plan Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--mode",
                        "memo",
                        "how do agent loops use memory",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("No profile provided", stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertEqual(len(run_manifests), 1)
            manifest = json.loads(run_manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "planned")
            self.assertEqual(manifest["attempt_count"], 0)
            self.assertTrue(manifest["resume_supported"])
            self.assertTrue((workspace.root / manifest["plan_path"]).exists())
            self.assertTrue((workspace.root / manifest["packet_path"]).exists())

            plan_text = (workspace.root / manifest["plan_path"]).read_text(encoding="utf-8")
            self.assertIn("Research Plan", plan_text)
            self.assertIn("Execute the prompt packet through the selected adapter profile.", plan_text)

    def test_research_resume_latest_reuses_existing_packet_and_updates_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Resume Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )

            self.assertEqual(
                main(["research", "--workspace", str(root), "how do agent loops use memory"]),
                0,
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["researcher"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Resumed Answer\\n\\nAgent loops use structured memory. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--resume",
                        "latest",
                        "--profile",
                        "researcher",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Resumed research run", stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertEqual(len(run_manifests), 1)
            manifest = json.loads(run_manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["attempt_count"], 1)
            self.assertEqual(manifest["resume_count"], 1)
            self.assertTrue(manifest["validation"]["passed"])
            answer_path = workspace.root / manifest["answer_path"]
            self.assertTrue(answer_path.exists())
            self.assertIn("Resumed Answer", answer_path.read_text(encoding="utf-8"))

    def test_research_fails_when_answer_cites_unknown_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Citation Validation Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["invalid"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Invalid Answer\\n\\nThis answer cites a missing source. [S9]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            exit_code = main(
                [
                    "research",
                    "--workspace",
                    str(root),
                    "--profile",
                    "invalid",
                    "how do agent loops use memory",
                ]
            )

            self.assertEqual(exit_code, 2)
            answer_path = workspace.wiki_dir / "queries" / "how-do-agent-loops-use-memory.md"
            self.assertTrue(answer_path.exists())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertTrue(run_manifests)
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertFalse(manifest["validation"]["passed"])
            self.assertIn("S9", " ".join(manifest["validation"]["errors"]))

    def test_research_fails_when_answer_contains_unsupported_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Unsupported Claims Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory. [seed]\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["unsupported"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Unsupported Answer\\n\\nAgent loops always require a vector database.')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            exit_code = main(
                [
                    "research",
                    "--workspace",
                    str(root),
                    "--profile",
                    "unsupported",
                    "how do agent loops use memory",
                ]
            )

            self.assertEqual(exit_code, 2)
            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertFalse(manifest["validation"]["passed"])
            self.assertIn("unsupported_claims", manifest["validation"]["checks"])
            self.assertTrue(manifest["validation"]["checks"]["unsupported_claims"]["errors"])

    def test_research_fails_answer_lint_when_missing_heading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Answer Lint Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["lintfail"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('Agent loops use structured memory. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            exit_code = main(
                [
                    "research",
                    "--workspace",
                    str(root),
                    "--profile",
                    "lintfail",
                    "how do agent loops use memory",
                ]
            )

            self.assertEqual(exit_code, 2)
            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertFalse(manifest["validation"]["passed"])
            self.assertTrue(manifest["validation"]["checks"]["answer_lint"]["errors"])

    def test_research_marks_conflicting_sources_as_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Conflict Detection Test")

            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["conflict"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Conflict Answer\\n\\nThe deployment model is local first. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "conflict",
                        "what is the deployment model",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertTrue(manifest["validation"]["passed"])
            self.assertTrue(manifest["validation"]["warnings"])
            self.assertEqual(manifest["status"], "completed_with_warnings")
            self.assertTrue(manifest["validation"]["checks"]["source_conflicts"]["warnings"])


if __name__ == "__main__":
    unittest.main()
