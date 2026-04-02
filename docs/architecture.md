# Cognisync Architecture

## Purpose

Cognisync is a generalized framework for a filesystem-native, LLM-operated knowledge base workflow.

The reference implementation focuses on the minimal durable substrate required to build serious tools around that workflow:

- a predictable workspace model
- deterministic indexing
- agent work packets
- lintable wiki integrity
- reusable renderers for reports and slide decks

## End-to-End Flow

```mermaid
flowchart TD
    A["Sources collected into raw/"] --> B["Scanner extracts metadata, links, tags, headings, and word counts"]
    B --> C["Index snapshot written to .cognisync/index.json"]
    C --> D["Planner creates compile and repair tasks"]
    D --> E["Prompt packets written to prompts/ or .cognisync/plans/"]
    E --> F["External LLM or agent team executes tasks"]
    F --> G["Wiki pages and outputs are written back into wiki/ and outputs/"]
    G --> H["Linter validates broken links, missing summaries, duplicate titles, and orphans"]
    H --> I["Search and query workflows generate reports and slides"]
    I --> G
```

## Component Model

```mermaid
flowchart LR
    W["Workspace"] --> S["Scanner"]
    W --> P["Planner"]
    W --> L["Linter"]
    W --> Q["Search Engine"]
    S --> I["Index Snapshot"]
    I --> P
    I --> L
    I --> Q
    P --> R["Prompt Packet Renderer"]
    Q --> O["Report and Marp Renderers"]
    R --> A["Command Adapter"]
    A --> X["External LLM CLI"]
```

## Module Dependency Graph

```mermaid
flowchart TD
    T["types.py"] --> C["config.py"]
    T --> W["workspace.py"]
    T --> S["scanner.py"]
    T --> P["planner.py"]
    T --> L["linter.py"]
    T --> Q["search.py"]
    T --> R["renderers.py"]
    C --> W
    W --> S
    S --> P
    S --> L
    S --> Q
    Q --> R
    P --> R
    C --> A["adapters.py"]
    R --> CLI["cli.py"]
    A --> CLI
    W --> CLI
    S --> CLI
    P --> CLI
    L --> CLI
    Q --> CLI
```

## Design Constraints

### Filesystem first

The filesystem is the primary database. Any derived state must be reconstructable from workspace files plus small deterministic metadata snapshots.

### Model agnostic

Cognisync does not hardcode a provider SDK. It emits prompt packets and exposes an adapter contract so users can plug in Codex, Claude Code, custom shell tools, or their own orchestration systems.

### Useful without a network

Search, linting, planning, and rendering should be helpful even before an LLM is connected.

### Durable outputs

Rendered Markdown, slide decks, and plans are kept as first-class artifacts so query work compounds over time instead of disappearing into chat history.

## Traceability Map

| Task | Deliverable | Diagram Anchor |
| --- | --- | --- |
| T1 | Workspace scaffold and configuration contract | Component Model |
| T2 | Artifact scanner and index snapshot | End-to-End Flow, Component Model |
| T3 | Compile and repair planner | End-to-End Flow, Component Model |
| T4 | Linter and integrity checks | End-to-End Flow |
| T5 | Search engine and query workflow | Component Model |
| T6 | Markdown and Marp renderers | Component Model |
| T7 | CLI and adapter integration points | Module Dependency Graph |
| T8 | Test suite and verification | Applies across all diagrams |
