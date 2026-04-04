import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class AccessTests(unittest.TestCase):
    def test_access_cli_manages_workspace_roles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Access Workspace"]), 0)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(main(["access", "list", "--workspace", str(root)]), 0)
            self.assertIn("Access Roster", stdout.getvalue())
            self.assertIn("local-operator", stdout.getvalue())

            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "alice",
                        "reviewer",
                        "--workspace",
                        str(root),
                        "--name",
                        "Alice Reviewer",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "bob",
                        "editor",
                        "--workspace",
                        str(root),
                        "--name",
                        "Bob Editor",
                    ]
                ),
                0,
            )
            self.assertEqual(main(["access", "revoke", "alice", "--workspace", str(root)]), 0)

            manifest_path = root / ".cognisync" / "access.json"
            self.assertTrue(manifest_path.exists())
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            members = {item["principal_id"]: item for item in payload["members"]}
            self.assertIn("local-operator", members)
            self.assertIn("bob", members)
            self.assertNotIn("alice", members)
            self.assertEqual(members["bob"]["role"], "editor")

    def test_access_cli_enforces_operator_role_for_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Access Workspace"]), 0)
            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "reviewer-1",
                        "reviewer",
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
                        "access",
                        "grant",
                        "alice",
                        "editor",
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("does not have permission", stderr.getvalue())

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "access",
                        "revoke",
                        "reviewer-1",
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("does not have permission", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
