# Contributing to Cognisync

Thanks for helping build Cognisync.

## Development Setup

```bash
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
/usr/bin/env PYTHONPYCACHEPREFIX=/tmp/cognisync-pyc python3 -m compileall src tests
```

## Contribution Expectations

- Keep the workspace model filesystem-first.
- Prefer deterministic behavior over hidden magic.
- Update tests before claiming a change is complete.
- Update docs when CLI commands, config structure, or adapter behavior changes.
- For non-trivial work, include diagrams and a traceable plan in the relevant docs.

## Pull Requests

- Keep PRs focused and reviewable.
- Explain the user-facing impact and validation steps.
- Add or update regression tests for behavior changes.
- Avoid bundling unrelated refactors with feature work.

## Release Policy

Cognisync is currently distributed as a GitHub-first source release.

That means:

- tagged releases are cut from `main`
- changelog entries are required for releases
- PyPI publication is deferred until the adapter and CLI contracts stabilize

See [Open Source Operations](docs/open-source-operations.md) for the current maintainer flow.
