import json
import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.planner import build_compile_plan
from cognisync.scanner import scan_workspace
from cognisync.workspace import Workspace


class GraphIntelligenceTests(unittest.TestCase):
    def test_scan_writes_entity_and_concept_candidate_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Graph Candidate Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Databases\n\nVector Databases improve recall for agent memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistent agent memory.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            graph_payload = json.loads((workspace.state_dir / "graph.json").read_text(encoding="utf-8"))

            entity_nodes = [node for node in graph_payload["nodes"] if node["kind"] == "entity"]
            concept_nodes = [node for node in graph_payload["nodes"] if node["kind"] == "concept_candidate"]

            self.assertTrue(any(node["title"] == "Vector Databases" for node in entity_nodes))
            vector_candidate = next(node for node in concept_nodes if node["title"] == "Vector Databases")
            self.assertEqual(vector_candidate["support_count"], 2)

            support_edges = [
                edge
                for edge in graph_payload["edges"]
                if edge["kind"] == "supports_concept" and edge["target"] == vector_candidate["id"]
            ]
            self.assertEqual(len(support_edges), 2)

    def test_planner_uses_concept_candidates_to_generate_concept_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Graph Planner Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Databases\n\nVector Databases improve recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)
            plan = build_compile_plan(snapshot)

            concept_tasks = [task for task in plan.tasks if task.kind == "create_concept_page"]
            outputs = {task.output_path for task in concept_tasks}
            self.assertIn("wiki/concepts/vector-databases.md", outputs)

    def test_graph_manifest_tracks_conflicting_claims_between_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Graph Conflict Test")

            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud Only\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            graph_payload = json.loads((workspace.state_dir / "graph.json").read_text(encoding="utf-8"))

            conflict_edges = [edge for edge in graph_payload["edges"] if edge["kind"] == "conflict"]
            self.assertTrue(conflict_edges)
            edge = conflict_edges[0]
            self.assertIn(edge["source"], {"raw/local.md", "raw/cloud.md"})
            self.assertIn(edge["target"], {"raw/local.md", "raw/cloud.md"})
            self.assertEqual(edge["subject"], "the deployment model")
            self.assertEqual(edge["verb"], "is")


if __name__ == "__main__":
    unittest.main()
