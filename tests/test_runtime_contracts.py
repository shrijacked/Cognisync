import io
import base64
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
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
    def test_remediate_research_replays_low_quality_runs_with_feedback_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Remediation Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["passer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ]
            )
            config.llm_profiles["failing"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops always require vector databases.')",
                ]
            )
            config.llm_profiles["healer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ]
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(workspace.root),
                        "--profile",
                        "passer",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertNotEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(workspace.root),
                        "--profile",
                        "failing",
                        "do agent loops always require vector databases",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "remediate",
                        "research",
                        "--workspace",
                        str(workspace.root),
                        "--profile",
                        "healer",
                        "--limit",
                        "1",
                    ]
                )

            self.assertEqual(exit_code, 0)
            remediation_dirs = sorted((workspace.outputs_dir / "reports" / "remediation-jobs").glob("remediation-*"))
            self.assertTrue(remediation_dirs)
            manifest_path = remediation_dirs[-1] / "manifest.json"
            prompt_path = remediation_dirs[-1] / "remediation-packet.md"
            answer_path = remediation_dirs[-1] / "answer.md"
            validation_report_path = remediation_dirs[-1] / "validation-report.md"
            self.assertTrue(manifest_path.exists())
            self.assertTrue(prompt_path.exists())
            self.assertTrue(answer_path.exists())
            self.assertTrue(validation_report_path.exists())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertTrue(manifest["validation"]["passed"])
            self.assertIn("citation_integrity", manifest["improvement_targets"])
            self.assertIn("grounding", manifest["improvement_targets"])
            self.assertIn("Remediation Prompt", prompt_path.read_text(encoding="utf-8"))
            self.assertIn("[S1]", answer_path.read_text(encoding="utf-8"))
            self.assertIn("Remediated 1 research run(s).", stdout.getvalue())

    def test_scan_writes_change_summary_for_new_conflicts_and_orphan_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Scan Change Summary Test")

            (workspace.raw_dir / "baseline.md").write_text(
                "# Baseline\n\nShared source.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["scan", "--workspace", str(workspace.root)]), 0)

            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "queries" / "agent-memory.md").write_text(
                "# Agent Memory\n\nThis page is not linked yet.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["scan", "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            summary_files = sorted((workspace.outputs_dir / "reports" / "change-summaries").glob("scan-*.md"))
            self.assertGreaterEqual(len(summary_files), 2)
            summary_texts = [path.read_text(encoding="utf-8") for path in summary_files]
            self.assertTrue(any("New conflicts" in text and "the deployment model is" in text for text in summary_texts))
            self.assertTrue(any("Orphan pages: `0 -> 1` (`+1`)" in text for text in summary_texts))
            self.assertTrue(any("## Graph Delta" in text for text in summary_texts))
            self.assertTrue(
                any(
                    "## Suggested Follow-Up Questions" in text and "deployment model" in text.lower()
                    for text in summary_texts
                )
            )
            self.assertIn("Wrote change summary", stdout.getvalue())

    def test_scan_change_summary_names_affected_graph_nodes_and_recompile_suggestions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Graph Change Intelligence Test")

            raw_path = workspace.raw_dir / "memory.md"
            summary_path = workspace.wiki_dir / "sources" / "memory.md"
            raw_path.write_text(
                "# Memory\n\n## Vector Databases\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            summary_path.write_text("# Memory\n\nInitial compiled summary.\n", encoding="utf-8")
            os.utime(raw_path, (1000, 1000))
            os.utime(summary_path, (2000, 2000))

            self.assertEqual(main(["scan", "--workspace", str(workspace.root)]), 0)

            raw_path.write_text(
                "# Memory\n\n"
                "## Vector Databases\n\n"
                "Agent memory uses vector databases.\n"
                "Vector Databases supports semantic recall.\n",
                encoding="utf-8",
            )
            os.utime(raw_path, (3000, 3000))

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["scan", "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            summary_files = sorted((workspace.outputs_dir / "reports" / "change-summaries").glob("scan-*.md"))
            self.assertGreaterEqual(len(summary_files), 2)
            summary_texts = [path.read_text(encoding="utf-8") for path in summary_files]
            summary_text = next(
                (text for text in summary_texts if "`raw/memory.md` modified source content" in text),
                "",
            )

            self.assertIn("## Changed Artifacts", summary_text)
            self.assertIn("`raw/memory.md` modified source content", summary_text)
            self.assertIn("summary target `wiki/sources/memory.md`", summary_text)
            self.assertIn("## Affected Graph Nodes", summary_text)
            self.assertIn("`assertion:agent-memory-uses-vector-databases` assertion support changed", summary_text)
            self.assertIn("`entity:vector-databases` entity support changed", summary_text)
            self.assertIn("## Recompilation Suggestions", summary_text)
            self.assertIn("`refresh_source_summary` `wiki/sources/memory.md` from `raw/memory.md`", summary_text)
            self.assertIn("Wrote change summary", stdout.getvalue())

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

    def test_maintain_writes_change_summary_for_concepts_and_merges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="Maintenance Change Summary Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Systems\n\n"
                "## Vector Database\n\n"
                "Vector Database improves recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Memory Systems\n\n"
                "## Vector Databases\n\n"
                "Vector Databases help persistence.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["maintain", "--workspace", str(workspace.root)])

            self.assertEqual(exit_code, 0)
            summary_files = sorted((workspace.outputs_dir / "reports" / "change-summaries").glob("maintenance-*.md"))
            self.assertTrue(summary_files)
            summary_text = summary_files[-1].read_text(encoding="utf-8")
            self.assertIn("New concept pages", summary_text)
            self.assertIn("wiki/concepts/vector-databases.md", summary_text)
            self.assertIn("Resolved merge decisions", summary_text)
            self.assertIn("vector database -> Vector Databases", summary_text)
            self.assertIn("Wrote change summary", stdout.getvalue())

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

    def test_new_ingest_sources_write_manifests_and_rank_for_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root / "workspace")
            workspace.initialize(name="New Ingest Source Manifest Test")

            source_root = root / "sources"
            source_root.mkdir()
            notebook = source_root / "memory-analysis.ipynb"
            notebook.write_text(
                json.dumps(
                    {
                        "cells": [
                            {
                                "cell_type": "markdown",
                                "metadata": {},
                                "source": ["# Memory Notebook\n\nNotebook experiment tracks recall."],
                            },
                            {
                                "cell_type": "code",
                                "metadata": {},
                                "source": ["recall_score = 0.91\n"],
                                "outputs": [],
                            },
                        ],
                        "metadata": {"language_info": {"name": "python"}},
                        "nbformat": 4,
                        "nbformat_minor": 5,
                    }
                ),
                encoding="utf-8",
            )
            dataset = source_root / "memory-corpus.json"
            dataset.write_text(
                json.dumps(
                    {
                        "name": "memory-corpus",
                        "features": ["question", "answer", "citation"],
                        "splits": {"train": 10, "test": 2},
                    }
                ),
                encoding="utf-8",
            )
            image_dir = source_root / "diagrams"
            image_dir.mkdir()
            (image_dir / "architecture.png").write_bytes(
                bytes.fromhex(
                    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
                    "0000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082"
                )
            )
            (image_dir / "architecture.txt").write_text("Architecture diagram for the memory pipeline.", encoding="utf-8")

            self.assertEqual(
                main(["ingest", "notebook", str(notebook), "--workspace", str(workspace.root), "--name", "memory-analysis"]),
                0,
            )
            self.assertEqual(
                main(["ingest", "dataset", str(dataset), "--workspace", str(workspace.root), "--name", "memory-corpus"]),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "ingest",
                        "image-folder",
                        str(image_dir),
                        "--workspace",
                        str(workspace.root),
                        "--name",
                        "architecture-sketches",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["scan", "--workspace", str(workspace.root)]), 0)

            sources_payload = json.loads(workspace.sources_manifest_path.read_text(encoding="utf-8"))
            by_kind = {source["source_kind"]: source for source in sources_payload["sources"]}
            self.assertEqual(by_kind["notebook"]["extraction_status"], "notebook_available")
            self.assertIn("raw/notebooks/memory-analysis.ipynb", by_kind["notebook"]["artifacts"])
            self.assertIn("raw/notebooks/memory-analysis.md", by_kind["notebook"]["artifacts"])
            self.assertEqual(by_kind["dataset"]["extraction_status"], "descriptor_available")
            self.assertIn("raw/datasets/memory-corpus.json", by_kind["dataset"]["artifacts"])
            self.assertEqual(by_kind["image_folder"]["extraction_status"], "assets_available")
            self.assertIn("raw/images/architecture-sketches-assets/architecture.png", by_kind["image_folder"]["captured_assets"])

            snapshot = scan_workspace(workspace)
            engine = SearchEngine.from_workspace(workspace, snapshot)
            notebook_hits = engine.search("notebook code experiment recall", limit=3)
            dataset_hits = engine.search("dataset features splits benchmark", limit=3)
            visual_hits = engine.search("architecture diagram image pipeline", limit=3)

            self.assertEqual(notebook_hits[0].path, "raw/notebooks/memory-analysis.md")
            self.assertEqual(dataset_hits[0].path, "raw/datasets/memory-corpus.md")
            self.assertEqual(visual_hits[0].path, "raw/images/architecture-sketches.md")

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
            self.assertIn("Wrote change summary", stdout.getvalue())

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
            self.assertIn("change_summary_path", manifest)
            summary_path = workspace.root / manifest["change_summary_path"]
            self.assertTrue(summary_path.exists())
            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("# Research Change Summary", summary_text)
            self.assertIn("Artifact count:", summary_text)

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
            self.assertIn("Wrote change summary", stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertEqual(len(run_manifests), 1)
            manifest = json.loads(run_manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "planned")
            self.assertEqual(manifest["attempt_count"], 0)
            self.assertTrue(manifest["resume_supported"])
            self.assertTrue((workspace.root / manifest["plan_path"]).exists())
            self.assertTrue((workspace.root / manifest["packet_path"]).exists())
            self.assertIn("change_summary_path", manifest)
            summary_path = workspace.root / manifest["change_summary_path"]
            self.assertTrue(summary_path.exists())
            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("# Research Change Summary", summary_text)
            self.assertIn("Artifact count:", summary_text)

            plan_text = (workspace.root / manifest["plan_path"]).read_text(encoding="utf-8")
            self.assertIn("Research Plan", plan_text)
            self.assertIn("Execute the prompt packet through the selected adapter profile.", plan_text)

    def test_research_job_profile_writes_note_artifacts_and_packet_instructions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Job Profile Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertEqual(len(run_manifests), 1)
            manifest = json.loads(run_manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["job_profile"], "literature-review")
            notes_dir = workspace.root / manifest["notes_dir"]
            self.assertTrue(notes_dir.exists())
            agent_plan_path = workspace.root / manifest["agent_plan_path"]
            source_packet_path = workspace.root / manifest["source_packet_path"]
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            self.assertTrue(agent_plan_path.exists())
            self.assertTrue(source_packet_path.exists())
            self.assertTrue(checkpoints_path.exists())
            execution_packet_paths = [workspace.root / path for path in manifest["execution_packet_paths"]]
            note_paths = [workspace.root / path for path in manifest["note_paths"]]
            self.assertGreaterEqual(len(note_paths), 4)
            self.assertTrue(any(path.name == "paper-matrix.md" for path in note_paths))
            self.assertTrue(all(path.exists() for path in note_paths))
            self.assertGreaterEqual(len(execution_packet_paths), 4)
            self.assertTrue(all(path.exists() for path in execution_packet_paths))

            plan_text = (workspace.root / manifest["plan_path"]).read_text(encoding="utf-8")
            packet_text = (workspace.root / manifest["packet_path"]).read_text(encoding="utf-8")
            source_packet_text = source_packet_path.read_text(encoding="utf-8")
            agent_plan_text = (notes_dir / "agent-plan.md").read_text(encoding="utf-8")
            agent_plan_payload = json.loads(agent_plan_path.read_text(encoding="utf-8"))
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            matrix_checkpoint = next(item for item in checkpoint_payload["steps"] if item["step_id"] == "build-paper-matrix")
            matrix_packet_path = workspace.root / matrix_checkpoint["execution_packet_path"]
            matrix_packet_text = matrix_packet_path.read_text(encoding="utf-8")
            self.assertIn("Job profile: literature-review", plan_text)
            self.assertIn("## Agent Assignments", plan_text)
            self.assertIn("Build paper matrix", plan_text)
            self.assertIn("Research job profile: literature-review", packet_text)
            self.assertIn("# Research Source Packet", source_packet_text)
            self.assertIn("# Research Agent Plan", agent_plan_text)
            self.assertEqual(agent_plan_payload["job_profile"], "literature-review")
            self.assertEqual(
                agent_plan_payload["run_manifest_path"],
                workspace.relative_path(run_manifests[0]),
            )
            self.assertEqual(agent_plan_payload["checkpoints_path"], manifest["checkpoints_path"])
            self.assertTrue(any(item["assignment_id"] == "assignment-build-paper-matrix" for item in agent_plan_payload["assignments"]))
            self.assertIn("# Research Execution Packet", matrix_packet_text)
            self.assertIn("Step: Build paper matrix", matrix_packet_text)
            self.assertIn(str(manifest["source_packet_path"]), matrix_packet_text)
            self.assertIn(str(manifest["packet_path"]), matrix_packet_text)
            self.assertEqual(matrix_checkpoint["assignment_id"], "assignment-build-paper-matrix")
            self.assertIn("assignment-build-paper-matrix", checkpoint_payload["assignment_statuses"])

    def test_research_step_commands_execute_and_review_execution_packets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Step Execution Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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

            config = load_config(workspace.config_path)
            config.llm_profiles["stepper"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    (
                        "import sys; sys.stdin.read(); "
                        "print('# Paper Matrix\\n\\n- memory helps agent loops persist findings [S1]')"
                    ),
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            list_stdout = io.StringIO()
            with redirect_stdout(list_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "list",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("build-paper-matrix", list_stdout.getvalue())
            self.assertIn("assignment=assignment-build-paper-matrix", list_stdout.getvalue())
            self.assertIn("agent=matrix-builder", list_stdout.getvalue())
            self.assertIn("capability=research", list_stdout.getvalue())
            self.assertIn("review-roles=reviewer", list_stdout.getvalue())
            self.assertIn("execution=not_run", list_stdout.getvalue())
            self.assertIn("review=unreviewed", list_stdout.getvalue())

            run_stdout = io.StringIO()
            with redirect_stdout(run_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "run",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                        "--step",
                        "build-paper-matrix",
                        "--profile",
                        "stepper",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Executed research step build-paper-matrix", run_stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            matrix_checkpoint = next(item for item in checkpoint_payload["steps"] if item["step_id"] == "build-paper-matrix")
            matrix_output_path = workspace.root / matrix_checkpoint["execution_output_path"]
            self.assertEqual(matrix_checkpoint["assignment_id"], "assignment-build-paper-matrix")
            self.assertEqual(matrix_checkpoint["execution_profile"], "stepper")
            self.assertEqual(matrix_checkpoint["execution_status"], "completed")
            self.assertEqual(matrix_checkpoint["review_status"], "pending_review")
            self.assertTrue(matrix_checkpoint["executed_at"])
            self.assertTrue(matrix_output_path.exists())
            self.assertIn("Paper Matrix", matrix_output_path.read_text(encoding="utf-8"))
            self.assertEqual(
                checkpoint_payload["assignment_statuses"]["assignment-build-paper-matrix"]["status"],
                "pending_review",
            )

            review_stdout = io.StringIO()
            with redirect_stdout(review_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "review",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                        "--step",
                        "build-paper-matrix",
                        "--status",
                        "approved",
                        "--reviewer",
                        "operator-1",
                        "--note",
                        "grounded and ready to reuse",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Reviewed research step build-paper-matrix as approved", review_stdout.getvalue())

            reviewed_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            reviewed_checkpoint = next(item for item in reviewed_payload["steps"] if item["step_id"] == "build-paper-matrix")
            self.assertEqual(reviewed_checkpoint["review_status"], "approved")
            self.assertEqual(reviewed_checkpoint["reviewed_by"], "operator-1")
            self.assertEqual(reviewed_checkpoint["review_note"], "grounded and ready to reuse")
            self.assertTrue(reviewed_checkpoint["reviewed_at"])

            config = load_config(workspace.config_path)
            config.llm_profiles["answerer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    (
                        "import sys; sys.stdin.read(); "
                        "print('# Research Memo\\n\\nAgent loops use memory to retain findings over time. [S1]')"
                    ),
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--resume",
                        "latest",
                        "--profile",
                        "answerer",
                    ]
                ),
                0,
            )

            resumed_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            resumed_checkpoint = next(item for item in resumed_payload["steps"] if item["step_id"] == "build-paper-matrix")
            self.assertEqual(resumed_checkpoint["execution_profile"], "stepper")
            self.assertEqual(resumed_checkpoint["review_status"], "approved")
            self.assertEqual(resumed_checkpoint["reviewed_by"], "operator-1")
            self.assertEqual(resumed_checkpoint["review_note"], "grounded and ready to reuse")

    def test_research_step_dispatch_routes_profiles_and_persists_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Step Dispatch Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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

            config = load_config(workspace.config_path)
            config.llm_profiles["alpha"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Step Artifact\\n\\nproduced by alpha [S1]')",
                ],
                stdin_source="prompt_file",
            )
            config.llm_profiles["beta"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Step Artifact\\n\\nproduced by beta [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            dispatch_stdout = io.StringIO()
            with redirect_stdout(dispatch_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "dispatch",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--profile-route",
                        "build-paper-matrix=beta",
                        "--profile-route",
                        "capture-open-questions=beta",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Executed 3 research step(s)", dispatch_stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            checkpoints_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            steps = {item["step_id"]: item for item in checkpoints_payload["steps"]}
            self.assertEqual(steps["build-working-set"]["execution_profile"], "alpha")
            self.assertEqual(steps["build-paper-matrix"]["execution_profile"], "beta")
            self.assertEqual(steps["capture-open-questions"]["execution_profile"], "beta")
            self.assertEqual(steps["build-working-set"]["execution_status"], "completed")
            self.assertEqual(steps["build-paper-matrix"]["review_status"], "pending_review")
            self.assertEqual(steps["capture-open-questions"]["review_status"], "pending_review")

            notes_dir = workspace.root / manifest["notes_dir"]
            self.assertIn("produced by alpha", (notes_dir / "working-set.md").read_text(encoding="utf-8"))
            self.assertIn("produced by beta", (notes_dir / "paper-matrix.md").read_text(encoding="utf-8"))
            self.assertIn("produced by beta", (notes_dir / "open-questions.md").read_text(encoding="utf-8"))

            dispatch_history = checkpoints_payload["dispatch_history"]
            self.assertEqual(len(dispatch_history), 1)
            dispatch_manifest_path = workspace.root / dispatch_history[0]
            self.assertTrue(dispatch_manifest_path.exists())
            dispatch_manifest = json.loads(dispatch_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(dispatch_manifest["status"], "completed")
            self.assertEqual(dispatch_manifest["executed_steps"], ["build-working-set", "build-paper-matrix", "capture-open-questions"])
            self.assertEqual(dispatch_manifest["step_profiles"]["build-working-set"], "alpha")
            self.assertEqual(dispatch_manifest["step_profiles"]["build-paper-matrix"], "beta")
            results_by_step = {item["step_id"]: item for item in dispatch_manifest["results"]}
            self.assertEqual(results_by_step["build-working-set"]["assignment_id"], "assignment-build-working-set")
            self.assertEqual(results_by_step["build-working-set"]["planned_worker_capability"], "research")
            self.assertEqual(results_by_step["build-working-set"]["planned_review_roles"], ["reviewer"])
            self.assertEqual(results_by_step["build-working-set"]["route_source"], "plan_default")
            self.assertEqual(results_by_step["build-paper-matrix"]["route_source"], "cli_override")

            second_dispatch_stdout = io.StringIO()
            with redirect_stdout(second_dispatch_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "dispatch",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Executed 0 research step(s)", second_dispatch_stdout.getvalue())
            self.assertIn("Skipped 3 research step(s)", second_dispatch_stdout.getvalue())

            second_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            self.assertEqual(len(second_payload["dispatch_history"]), 2)

    def test_research_step_dispatch_uses_plan_default_profile_when_no_route_is_given(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Step Plan Defaults Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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
            config.llm_profiles["beta"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Step Artifact\\n\\nproduced by beta [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--job-profile",
                        "literature-review",
                        "--profile",
                        "alpha",
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
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "beta",
                    ]
                ),
                0,
            )

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            dispatch_manifest_path = workspace.root / checkpoint_payload["dispatch_history"][-1]
            dispatch_manifest = json.loads(dispatch_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(dispatch_manifest["step_profiles"]["build-working-set"], "alpha")
            self.assertEqual(dispatch_manifest["step_profiles"]["build-paper-matrix"], "alpha")
            self.assertEqual(dispatch_manifest["step_profiles"]["capture-open-questions"], "alpha")
            for record in dispatch_manifest["results"]:
                if record["status"] == "skipped":
                    continue
                self.assertEqual(record["route_source"], "plan_default")

    def test_research_step_dispatch_can_queue_hosted_jobs_and_run_them_through_jobs_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Hosted Research Step Dispatch Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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
                        str(root),
                        "--job-profile",
                        "literature-review",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )

            dispatch_stdout = io.StringIO()
            with redirect_stdout(dispatch_stdout):
                exit_code = main(
                    [
                        "research-step",
                        "dispatch",
                        "--workspace",
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--hosted",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Queued 4 research step job(s)", dispatch_stdout.getvalue())

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            dispatch_manifest_path = workspace.root / checkpoint_payload["dispatch_history"][-1]
            dispatch_manifest = json.loads(dispatch_manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(dispatch_manifest["dispatch_mode"], "hosted")
            self.assertEqual(
                dispatch_manifest["queued_steps"],
                ["build-working-set", "build-paper-matrix", "capture-open-questions", "execute-profile"],
            )
            self.assertEqual(dispatch_manifest["status"], "queued")

            queue_payload = json.loads(workspace.job_queue_manifest_path.read_text(encoding="utf-8"))
            research_step_jobs = [job for job in queue_payload["jobs"] if job["job_type"] == "research_step"]
            self.assertEqual(len(research_step_jobs), 4)
            self.assertTrue(all(job["status"] == "queued" for job in research_step_jobs))
            self.assertTrue(all(job["worker_capability"] == "research" for job in research_step_jobs))
            job_manifest_payloads = [
                json.loads((workspace.job_manifests_dir / f"{job['job_id']}.json").read_text(encoding="utf-8"))
                for job in research_step_jobs
            ]
            params_by_step = {job["parameters"]["step_id"]: job["parameters"] for job in job_manifest_payloads}
            self.assertEqual(params_by_step["build-working-set"]["assignment_id"], "assignment-build-working-set")
            self.assertEqual(params_by_step["execute-profile"]["assignment_id"], "assignment-execute-profile")
            self.assertEqual(params_by_step["execute-profile"]["planned_review_roles"], ["reviewer", "operator"])
            self.assertEqual(params_by_step["execute-profile"]["route_source"], "plan_default")

            list_stdout = io.StringIO()
            with redirect_stdout(list_stdout):
                self.assertEqual(
                    main(
                        [
                            "research-step",
                            "list",
                            "--workspace",
                            str(root),
                            "--run",
                            "latest",
                        ]
                    ),
                    0,
                )
            self.assertIn("job-status=queued", list_stdout.getvalue())

            for _ in range(4):
                self.assertEqual(
                    main(
                        [
                            "jobs",
                            "run-next",
                            "--workspace",
                            str(root),
                            "--worker-id",
                            "research-worker",
                            "--capability",
                            "research",
                        ]
                    ),
                    0,
                )

            updated_queue_payload = json.loads(workspace.job_queue_manifest_path.read_text(encoding="utf-8"))
            completed_jobs = [job for job in updated_queue_payload["jobs"] if job["job_type"] == "research_step"]
            self.assertEqual(len(completed_jobs), 4)
            self.assertTrue(all(job["status"] == "completed" for job in completed_jobs))

            updated_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            steps = {item["step_id"]: item for item in updated_payload["steps"]}
            self.assertEqual(steps["build-working-set"]["execution_status"], "completed")
            self.assertEqual(steps["build-paper-matrix"]["execution_status"], "completed")
            self.assertEqual(steps["capture-open-questions"]["execution_status"], "completed")
            self.assertEqual(steps["execute-profile"]["execution_status"], "completed")
            self.assertEqual(steps["execute-profile"]["review_status"], "pending_review")
            self.assertEqual(
                updated_payload["assignment_statuses"]["assignment-execute-profile"]["status"],
                "pending_review",
            )

            answer_path = workspace.root / manifest["answer_path"]
            self.assertTrue(answer_path.exists())
            self.assertIn("Agent loops use memory", answer_path.read_text(encoding="utf-8"))

            updated_dispatch_manifest = json.loads(dispatch_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(updated_dispatch_manifest["status"], "completed")
            self.assertEqual(
                updated_dispatch_manifest["executed_steps"],
                ["build-working-set", "build-paper-matrix", "capture-open-questions", "execute-profile"],
            )
            updated_results = {item["step_id"]: item for item in updated_dispatch_manifest["results"]}
            self.assertEqual(updated_results["build-working-set"]["status"], "completed")
            self.assertEqual(updated_results["execute-profile"]["status"], "completed")
            self.assertEqual(updated_results["execute-profile"]["profile"], "alpha")

    def test_research_resume_without_profile_finalizes_from_approved_checkpoint_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Resume Reconciliation Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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
                        str(root),
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
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--hosted",
                    ]
                ),
                0,
            )

            for _ in range(4):
                self.assertEqual(
                    main(
                        [
                            "jobs",
                            "run-next",
                            "--workspace",
                            str(root),
                            "--worker-id",
                            "research-worker",
                            "--capability",
                            "research",
                        ]
                    ),
                    0,
                )

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
                            str(root),
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
                        str(root),
                        "--resume",
                        "latest",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Resumed research run", stdout.getvalue())

            manifest_path = sorted((workspace.state_dir / "runs").glob("research-*.json"))[-1]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["resume_strategy"], "checkpoint_finalize")
            self.assertEqual(manifest["reconciled_from_step_id"], "execute-profile")
            self.assertEqual(manifest["reconciled_from_assignment_id"], "assignment-execute-profile")
            self.assertEqual(manifest["reconciled_from_output_path"], manifest["answer_path"])
            self.assertTrue(manifest["validation"]["passed"])

            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            steps = {item["step_id"]: item for item in checkpoint_payload["steps"]}
            self.assertEqual(steps["validate-citations"]["status"], "completed")
            self.assertEqual(steps["file-answer"]["status"], "completed")
            self.assertEqual(
                checkpoint_payload["assignment_statuses"]["assignment-validate-citations"]["status"],
                "completed",
            )
            self.assertEqual(
                checkpoint_payload["assignment_statuses"]["assignment-file-answer"]["status"],
                "completed",
            )

    def test_research_resume_without_profile_reports_review_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Resume Blocker Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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
                        str(root),
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
                        str(root),
                        "--run",
                        "latest",
                        "--default-profile",
                        "alpha",
                        "--hosted",
                    ]
                ),
                0,
            )

            for _ in range(4):
                self.assertEqual(
                    main(
                        [
                            "jobs",
                            "run-next",
                            "--workspace",
                            str(root),
                            "--worker-id",
                            "research-worker",
                            "--capability",
                            "research",
                        ]
                    ),
                    0,
                )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--resume",
                        "latest",
                    ]
                )

            self.assertEqual(exit_code, 2)
            self.assertIn("build-working-set", stderr.getvalue())
            self.assertIn("pending review", stderr.getvalue().lower())
            self.assertIn("execute-profile", stderr.getvalue())

    def test_research_resume_backfills_missing_agent_plan_and_assignment_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Agent Plan Backfill Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
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

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest_path = run_manifests[-1]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            notes_dir = workspace.root / manifest["notes_dir"]
            plan_json_path = workspace.root / manifest["plan_json_path"]
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            agent_plan_path = workspace.root / manifest["agent_plan_path"]

            legacy_manifest = dict(manifest)
            legacy_manifest.pop("agent_plan_path", None)
            manifest_path.write_text(json.dumps(legacy_manifest, indent=2, sort_keys=True), encoding="utf-8")

            legacy_plan = json.loads(plan_json_path.read_text(encoding="utf-8"))
            legacy_plan.pop("agent_plan_path", None)
            legacy_plan.pop("assignments", None)
            for step in legacy_plan["steps"]:
                step.pop("assignment_id", None)
            plan_json_path.write_text(json.dumps(legacy_plan, indent=2, sort_keys=True), encoding="utf-8")

            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            checkpoint_payload.pop("assignment_statuses", None)
            for step in checkpoint_payload["steps"]:
                step.pop("assignment_id", None)
            checkpoints_path.write_text(json.dumps(checkpoint_payload, indent=2, sort_keys=True), encoding="utf-8")

            agent_plan_path.unlink()
            (notes_dir / "agent-plan.md").unlink()

            config = load_config(workspace.config_path)
            config.llm_profiles["answerer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings over time. [S1]')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--resume",
                        "latest",
                        "--profile",
                        "answerer",
                    ]
                ),
                0,
            )

            updated_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            updated_plan = json.loads(plan_json_path.read_text(encoding="utf-8"))
            updated_checkpoints = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            self.assertIn("agent_plan_path", updated_manifest)
            self.assertTrue((workspace.root / updated_manifest["agent_plan_path"]).exists())
            self.assertIn("agent_plan_path", updated_plan)
            self.assertTrue(updated_plan["assignments"])
            self.assertTrue(all(step.get("assignment_id") for step in updated_plan["steps"] if step["kind"] in {"build_working_set", "build_paper_matrix", "capture_open_questions", "execute_profile", "validate_citations", "file_answer"}))
            self.assertTrue(
                all(step.get("assignment_id") for step in updated_checkpoints["steps"] if step["kind"] in {"build_working_set", "build_paper_matrix", "capture_open_questions", "execute_profile", "validate_citations", "file_answer"})
            )
            self.assertIn("assignment-build-paper-matrix", updated_checkpoints["assignment_statuses"])

    def test_research_resume_latest_preserves_job_profile_notes_and_validation_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Research Resume Profile Test")

            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--job-profile",
                        "contradiction-finding",
                        "what is the deployment model",
                    ]
                ),
                0,
            )

            config = load_config(workspace.config_path)
            config.llm_profiles["researcher"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Conflict Answer\\n\\nThe sources disagree about the deployment model. Local first appears in [S1], while cloud only appears in [S2].')",
                ],
                stdin_source="prompt_file",
            )
            save_config(workspace.config_path, config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--resume",
                        "latest",
                        "--profile",
                        "researcher",
                    ]
                ),
                0,
            )

            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            self.assertEqual(len(run_manifests), 1)
            manifest = json.loads(run_manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["job_profile"], "contradiction-finding")
            self.assertTrue(manifest["validation"]["passed"])
            self.assertTrue(any(path.endswith("claim-ledger.md") for path in manifest["note_paths"]))
            checkpoints_path = workspace.root / manifest["checkpoints_path"]
            source_packet_path = workspace.root / manifest["source_packet_path"]
            self.assertTrue(checkpoints_path.exists())
            self.assertTrue(source_packet_path.exists())
            validation_report_path = workspace.root / manifest["validation_report_path"]
            self.assertTrue(validation_report_path.exists())
            validation_text = validation_report_path.read_text(encoding="utf-8")
            checkpoint_payload = json.loads(checkpoints_path.read_text(encoding="utf-8"))
            self.assertIn("Validation Status", validation_text)
            self.assertIn("completed", validation_text.lower())
            execute_step = next(item for item in checkpoint_payload["steps"] if item["step_id"] == "execute-profile")
            self.assertEqual(execute_step["status"], "completed")

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
            self.assertEqual(manifest["resume_strategy"], "adapter_rerun")
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

    def test_research_fails_when_conflicts_are_not_acknowledged(self) -> None:
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

            self.assertEqual(exit_code, 2)
            run_manifests = sorted((workspace.state_dir / "runs").glob("research-*.json"))
            manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
            self.assertFalse(manifest["validation"]["passed"])
            self.assertEqual(manifest["status"], "failed_validation")
            self.assertTrue(manifest["validation"]["checks"]["source_conflicts"]["errors"])

    def test_research_accepts_conflicts_when_answer_surfaces_both_sides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Conflict Resolution Answer Test")

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
                    (
                        "import sys; sys.stdin.read(); "
                        "print('# Conflict Answer\\n\\nThe sources disagree about the deployment model. "
                        "One source says it is local first [S1], while another says it is cloud only [S2].')"
                    ),
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
            self.assertFalse(manifest["validation"]["checks"]["source_conflicts"]["errors"])


if __name__ == "__main__":
    unittest.main()
