from pathlib import Path
import unittest


class CiWorkflowTests(unittest.TestCase):
    def test_ci_generates_demo_workspace_before_health_checks(self) -> None:
        workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

        demo_index = workflow.index("Smoke test demo workspace generation")
        doctor_index = workflow.index("Smoke test doctor on demo workspace")
        lint_index = workflow.index("Smoke test lint on demo workspace")

        self.assertLess(
            demo_index,
            doctor_index,
            "CI should generate the demo workspace before running doctor.",
        )
        self.assertLess(
            demo_index,
            lint_index,
            "CI should generate the demo workspace before running lint.",
        )
        self.assertIn("python -m cognisync demo /tmp/cognisync-demo", workflow)
        self.assertIn("python -m cognisync doctor --workspace /tmp/cognisync-demo --strict", workflow)
        self.assertIn("python -m cognisync lint --workspace /tmp/cognisync-demo --strict", workflow)


if __name__ == "__main__":
    unittest.main()
