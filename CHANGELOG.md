# Changelog

## Unreleased

- added a root `AGENTS.md` workspace schema so every Cognisync workspace now carries an explicit agent-facing contract alongside the corpus
- added a root `log.md` activity ledger so init, ingest, lint, compile, research, and maintenance work leave a readable append-only trail
- added regenerated wiki navigation catalogs at `wiki/index.md`, `wiki/sources.md`, `wiki/concepts.md`, and `wiki/queries.md`
- taught workspace refresh flows to rebuild those navigation surfaces before persisting `.cognisync/index.json`
- taught navigation catalogs to behave like metadata instead of corpus assertions, so generated indexes no longer distort review queues, orphan deltas, or synthetic QA exports
- taught query-page backlinking to persist through the generated catalogs by rendering explicit review-approved wikilinks back into `wiki/queries.md`
- added `cognisync export jsonl` so research runs can be emitted as portable JSONL dataset artifacts
- added `cognisync export training-bundle` so research runs can be packaged as label-bearing training datasets
- added `cognisync export finetune-bundle` so research runs and synthetic graph-derived examples can be emitted together as supervised and retrieval datasets
- taught `cognisync export finetune-bundle` to include validated remediation corrections in its supervised datasets and provider-ready exports
- added `--provider-format openai-chat` to `cognisync export finetune-bundle` so the same bundle can emit OpenAI-ready chat records without a separate conversion step
- added `cognisync export feedback-bundle` so low-quality research runs can be turned into remediation-ready records for correction loops
- added `cognisync remediate research` so low-quality runs can be replayed through correction prompts and validated without overwriting the original artifacts
- added `cognisync export correction-bundle` so validated remediation jobs can be exported as correction-training records with previous-response context
- added `cognisync export training-loop-bundle` so evaluation, feedback, correction, and finetune artifacts can ship together as one portable training package
- added `cognisync improve research` so the remediation loop and training-loop bundle refresh can run as one operator action
- added `cognisync notify list` plus `.cognisync/notifications.json` so jobs, runs, connectors, and review state now materialize into a durable operator inbox
- added `cognisync jobs` so research and improvement work can be queued, persisted, and executed later through local control-plane style manifests
- added `cognisync sync export` and `sync import` so file-native workspaces can move between operators as portable bundles
- added `cognisync access list|grant|revoke` plus `.cognisync/access.json` so workspace roles now persist as first-class file-native state
- added `cognisync collab list|request-review|comment|approve|request-changes|resolve` plus `.cognisync/collaboration.json` so artifact-level review threads now persist as first-class workspace state
- added `cognisync audit list` plus `.cognisync/audit.json` so runs, jobs, sync events, connectors, and access state now materialize into a readable audit index
- added `cognisync usage report` plus `.cognisync/usage.json` so the workspace can derive counts for runs, jobs, connectors, sync volume, roles, collaboration, and storage
- taught sync bundle manifests to declare included state manifests, including `.cognisync/access.json`
- added `cognisync jobs retry` so terminal jobs can be re-queued with explicit lineage to the original manifest
- added `cognisync jobs enqueue compile|lint|maintain` and `jobs work` so the queue now covers more of the operator loop and can drain sequentially like a lightweight worker
- added `cognisync jobs claim-next` plus worker leases so queued jobs can be claimed, resumed, and reclaimed across worker identities without inventing a second queue system
- added `cognisync jobs heartbeat` so active workers can renew leased jobs without dropping ownership
- added `cognisync jobs workers` plus `.cognisync/jobs/workers.json` so queue ownership now materializes as a file-native worker registry
- added `.cognisync/sync/history.json` and per-event sync manifests so workspace handoffs now leave an audit trail
- added `cognisync connector add|list|sync` plus queued `connector-sync` jobs so remote-style source definitions now live as workspace manifests too
- added `cognisync connector sync-all` plus queued `connector-sync-all` jobs so the control plane can refresh the connector registry in one pass
- added `cognisync connector subscribe|unsubscribe` plus scheduled-only connector sync selection so connector pulls can run on durable file-native subscription metadata
- taught `cognisync notify list` to surface due connector subscriptions in the operator inbox
- added `cognisync export presentations` so generated slide decks and companion reports can be bundled for downstream sharing
- added `cognisync eval research` so persisted research runs can be scored into Markdown and JSON evaluation reports
- expanded `cognisync eval research` with dimensioned quality metrics for grounding, citation integrity, retrieval coverage, structure, artifact completeness, and contradiction handling
- added `cognisync synth qa` and `cognisync synth contrastive` so the assertion graph can emit deterministic synthetic QA and retrieval data
- added `research --job-profile` so question-driven runs can scaffold profile-specific intermediate notes and validation reports
- added source-packet and checkpoints artifacts inside each research job workspace so resumed runs keep a fuller execution bundle
- added source-backed assertion nodes and artifact support edges to `.cognisync/graph.json`
- added claim-level fact blocks to query and research reports so grounded assertions render separately from narrative synthesis
- grounded accepted concept pages with assertion-backed evidence sections instead of source links alone
- expanded `cognisync ui review` with source-coverage panels, compile-health summaries, run timelines, concept-graph pages, graph-node drilldowns, run-detail pages, artifact previews, lightweight filters, and local review actions when served
- expanded `cognisync ui review` again with job-queue and sync-history panels, filterable explorers, and static job-detail and sync-detail pages
- expanded `cognisync ui review` again with connector-registry panels, connector-detail pages, and live `run next job` and `sync connector` actions when served locally
- expanded `cognisync ui review` again with a live `sync all connectors` action so served dashboards can refresh the unsynced registry directly
- expanded `cognisync ui review` again with a notifications panel sourced from `.cognisync/notifications.json`
- expanded `cognisync ui review` again with a workspace-access panel sourced from `.cognisync/access.json`
- expanded `cognisync ui review` again with a collaboration panel sourced from `.cognisync/collaboration.json` and live collaboration actions when served locally
- expanded `cognisync ui review` again with audit-history and usage-summary panels sourced from `.cognisync/audit.json` and `.cognisync/usage.json`
- taught served review dashboards to run as an explicit workspace actor with role-gated live actions, so review work can be browser-driven without giving every actor operator powers
- expanded `cognisync ui review` with a worker panel sourced from `.cognisync/jobs/workers.json`
- taught `cognisync sync export` and `sync import` to require explicit operator actors and persist those principals in sync bundle manifests plus `.cognisync/sync/history.json`
- taught `access` mutations, connector mutations, and job queue submission/retry commands to accept `--actor-id` and require operator principals
- taught queued job manifests and connector manifests to persist the acting principal, so scheduler intent and connector ownership are durable in the filesystem state
- added `cognisync review export` so the open review queue, dismissal ledger, and review-action state can be handed to other tools as a deterministic JSON artifact
- added research change-summary artifacts so planned, resumed, and completed research runs all leave a readable corpus delta behind
- enriched change summaries with graph deltas and suggested follow-up questions
- added stale-summary lint checks and `refresh_source_summary` compile tasks so outdated source summaries surface as actionable maintenance work
- added `.cognisync/control-plane.json` plus `cognisync control-plane status|invite|accept-invite|issue-token|list-tokens|revoke-token|scheduler-tick|serve` so invites, scoped bearer tokens, scheduler ticks, and a lightweight HTTP server now sit on top of the same file-native workspace state
- added `cognisync worker remote` so queued jobs can be drained through the hosted-alpha HTTP surface by a separate worker process without inventing a second runtime
- taught sync bundle manifests to declare `.cognisync/control-plane.json` in `state_manifests`, so the hosted-alpha layer travels with exported workspaces

## v0.1.4 - 2026-04-03

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
- added `review dismiss` with persisted reasons so operators can intentionally close low-value queue items and keep them out of future maintenance runs
- added workspace change-summary artifacts after `scan`, `ingest`, and `maintain` so corpus deltas are readable without diffing manifests by hand
- added `review reopen` so dismissed queue items can be restored without editing manifest files by hand
- added configurable maintenance policy controls in workspace config and one-off CLI overrides for concept support thresholds and deny lists
- added `review list-dismissed` and `review clear-dismissed` so the dismissal ledger is manageable from the CLI
- taught `doctor` to surface maintenance policy state and warn when settings are permissive enough to create noisier maintenance runs

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
