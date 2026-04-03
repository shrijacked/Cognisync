# Changelog

## Unreleased

- added stable `.cognisync/sources.json` and `.cognisync/graph.json` manifests so scans persist grouped source and graph state
- added compile and research run manifests under `.cognisync/runs/`
- added citation validation for research answers and fail-fast handling for unknown source ids
- added explicit `cognisync research --mode ...` support for `wiki`, `report`, `memo`, `brief`, and `slides`
- improved deterministic retrieval with source-type-aware ranking and surfaced retrieval reasons in rendered artifacts
- added remote Git repo ingest plus `ingest urls` and `ingest sitemap` so larger web source sets can land through the CLI without wrapper scripts
- added persisted research plans and `research --resume` so question-driven jobs can be planned first and executed or retried later
- added stronger research verification for unsupported claims, answer lint, and conflicting source statements
- enriched `.cognisync/graph.json` with entity nodes, concept candidates, and conflict edges, and taught compile planning to use concept candidates beyond explicit tags
- added `.cognisync/review-queue.json` plus `cognisync review` so graph intelligence becomes a durable operator queue
- added graph-aware lint checks for missing raw metadata, duplicate concept pages, and conflicting source claims
- added `.cognisync/review-actions.json`, `review accept-concept`, and `review resolve-merge` so review items can be applied deterministically
- added `cognisync maintain` to auto-apply open concept and merge actions and persist a maintenance run manifest
- added `review apply-backlink` and `review file-conflict` so the remaining deterministic queue items are actionable too
- expanded `maintain` to apply backlink routing and conflict filing before writing its run manifest
- tightened `maintain` so low-signal concept candidates stay in the queue instead of being auto-accepted
- upgraded research conflict validation so conflicting retrieved sources must be surfaced with citations from both sides

## v0.1.3 - 2026-04-03

- added a built-in Claude Code CLI adapter preset for headless prompt-packet execution
- extended adapter command templating with `{prompt_text}` so CLIs can consume full prompt packets as command arguments
- fixed GitHub Actions smoke checks to generate a fresh demo workspace before running `doctor` and `lint`
- deepened ingest with PDF text sidecars, URL image capture, repository tree snapshots, and manifest-driven batch ingest
- taught compile packets to include richer input-context excerpts from raw artifacts
- upgraded query reports with inline source ids and source blocks
- added `cognisync research` as an opinionated question-to-artifact orchestration surface

## v0.1.2 - 2026-04-03

- enriched `ingest url` with page metadata, heading inventories, discovered links, and content stats
- enriched `ingest repo` with repository stats, language signals, recent commits, and richer manifests

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
