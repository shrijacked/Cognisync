# Cognisync

[![CI](https://github.com/shrijacked/Cognisync/actions/workflows/ci.yml/badge.svg)](https://github.com/shrijacked/Cognisync/actions/workflows/ci.yml)

Cognisync is a filesystem-first framework for building LLM-maintained knowledge bases.

It turns the workflow described by Andrej Karpathy into a reusable open source system:

1. Collect raw source material into a workspace.
2. Index and normalize that material into a deterministic manifest.
3. Generate structured work packets for LLM agents to compile a wiki.
4. Lint the resulting knowledge base for integrity problems.
5. Answer questions by searching the corpus and rendering outputs back into Markdown, slides, and other artifacts.

The goal is not to replace your favorite model or agent runner. The goal is to provide the workspace model, orchestration contracts, indexing primitives, and output formats that let people build serious tooling around this pattern.

## Core Ideas

- Filesystem-native: `raw/`, `wiki/`, and `outputs/` stay readable in tools like Obsidian.
- LLM-compatible: the framework produces prompt packets and execution plans for external LLM CLIs.
- Incremental: every scan, lint pass, query, and report can be filed back into the workspace.
- Deterministic where possible: indexing, search, linting, and report scaffolding work without network access.
- Extensible: users can write adapters, renderers, and orchestration layers on top of the core contracts.

## Workspace Layout

```text
workspace/
├── raw/
│   └── ... source documents, repos, datasets, images
├── wiki/
│   ├── index.md
│   ├── sources/
│   ├── concepts/
│   └── queries/
├── outputs/
│   ├── reports/
│   └── slides/
├── prompts/
└── .cognisync/
    ├── config.json
    ├── index.json
    └── plans/
```

## What Ships In This Reference Implementation

- Workspace scaffolding
- Deterministic corpus scanner and manifest builder
- Markdown-aware search over `raw/` and `wiki/`
- Compile planner for missing summaries, concept pages, and repair work
- Knowledge-base linter for broken links, missing summaries, and duplicate titles
- Markdown and Marp report renderers
- Command adapter contracts for wiring in external LLM CLIs
- A tested Python API and CLI

## Quickstart

```bash
python3 -m pip install -e .
cognisync init .
cognisync adapter list
cognisync adapter install codex --profile codex
cognisync scan
cognisync plan
cognisync lint
cognisync query "What are the main themes in this workspace?" --slides
```

## Built-In Adapter Example

Cognisync now ships with real Codex and Gemini CLI presets so users do not have to guess at the adapter shape:

```bash
cognisync adapter install codex --profile codex
cognisync adapter install gemini --profile gemini

cognisync run-packet prompts/compile-plan.md --profile codex --output-file outputs/reports/compile-pass.md
cognisync run-packet prompts/query-what-are-the-main-themes-in-this-workspace.md --profile gemini --output-file outputs/reports/gemini-brief.md
```

The built-in `codex` preset:

- streams the prompt packet to `codex exec` over stdin
- runs Codex in the current workspace root
- uses `--output-last-message` when you pass `--output-file`

The built-in `gemini` preset:

- streams the prompt packet to Gemini CLI over stdin
- runs Gemini in non-interactive mode using `--prompt`
- captures stdout into `--output-file` through Cognisync when you request a file output

## Release Strategy

`v0.1.0` is intentionally a GitHub-first source release.

The package metadata is already in place, but the project is staying repo-first for now so the adapter contract, CLI surface, and contributor workflow can stabilize before a PyPI push. The current release policy is documented in [Open Source Operations](docs/open-source-operations.md).

## Design Philosophy

Cognisync assumes the knowledge base itself is the product surface.

Instead of hiding data behind a vector database or a proprietary UI, it keeps the corpus inspectable and durable:

- raw inputs are preserved
- compiled wiki pages are versioned files
- generated reports are first-class artifacts
- agent work is represented as packets and plans that other tools can consume

This makes the system easy to automate, easy to audit, and easy to publish.

## Architecture

The implementation is documented in:

- [Architecture](docs/architecture.md)
- [Execution Plan](docs/execution-plan.md)
- [Open Source Operations](docs/open-source-operations.md)

## Community

- [Contributing Guide](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Changelog](CHANGELOG.md)

## Roadmap

- Multi-agent orchestration profiles
- Native repository and dataset ingestion adapters
- Richer semantic extraction and entity graphs
- Continuous health checks and auto-remediation loops
- Fine-tuning and synthetic dataset export pipelines

## License

MIT
