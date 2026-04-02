# Changelog

## v0.1.1 - 2026-04-03

- added `cognisync doctor` for workspace and adapter readiness checks
- added `cognisync ingest file`, `pdf`, `url`, and `repo` commands for pulling material into `raw/`
- added `cognisync compile` to run the scan, plan, execute, and lint loop as one command
- added a built-in Gemini CLI adapter preset so the framework now ships with multiple frontier-model CLI integrations
- documented dual built-in adapter flows for Codex and Gemini in the README
- added a polished `cognisync demo` flow plus a checked-in example garden for faster onboarding

## v0.1.0 - 2026-04-03

Initial public release.

- shipped the Cognisync reference implementation with workspace scaffolding, scanning, planning, linting, search, rendering, and CLI orchestration
- added a built-in Codex CLI adapter preset with stdin packet streaming and optional output-file capture
- added GitHub Actions CI for pushes and pull requests
- added contribution docs, issue templates, a pull request template, and a code of conduct
- documented the GitHub-first release strategy and deferred PyPI publishing until the adapter and CLI surfaces stabilize
