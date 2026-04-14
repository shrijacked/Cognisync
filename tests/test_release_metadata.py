import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _match(pattern: str, text: str) -> str:
    match = re.search(pattern, text, flags=re.MULTILINE)
    if not match:
        raise AssertionError(f"Could not match release metadata pattern: {pattern}")
    return match.group(1)


class ReleaseMetadataTests(unittest.TestCase):
    def test_package_versions_match_latest_changelog_release(self) -> None:
        latest_release = _match(r"^## v(\d+\.\d+\.\d+) - ", _read("CHANGELOG.md"))

        self.assertEqual(_match(r'^version = "([^"]+)"$', _read("pyproject.toml")), latest_release)
        self.assertEqual(_match(r'^__version__ = "([^"]+)"$', _read("src/cognisync/__init__.py")), latest_release)

    def test_release_docs_name_latest_documented_release(self) -> None:
        latest_release = _match(r"^## v(\d+\.\d+\.\d+) - ", _read("CHANGELOG.md"))

        self.assertIn(f"`v{latest_release}`", _read("README.md"))
        self.assertIn(f"`v{latest_release}`", _read("docs/open-source-operations.md"))


if __name__ == "__main__":
    unittest.main()
