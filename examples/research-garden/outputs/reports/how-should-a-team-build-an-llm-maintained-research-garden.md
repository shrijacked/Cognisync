# Research Brief: How should a team build an LLM-maintained research garden?

Generated: 2026-04-02T21:44:53+00:00

## Top Sources

1. [Research Garden Brief](../../wiki/queries/research-garden-brief.md)
   Score: 51.5162
   Snippet: # Research Garden Brief  This seeded answer shows how a team can treat the wiki as the durable product surface.  ## Core Pattern  - collect source material into `raw/` - compile summaries and concepts into `wiki/` - ask reusable questions and fi

2. [Knowledge Base Index](../../wiki/index.md)
   Score: 10.0474
   Snippet: # Knowledge Base Index  Cognisync demo garden for an LLM-maintained research workflow.  ## Entry Points  - &#91;Sources&#93;(sources.md) - &#91;Concepts&#93;(concepts.md) - &#91;Queries&#93;(queries.md) - &#91;&#91;knowledge-gardens&#93;&#93; - &#91;&#91;agent-

3. [Knowledge Garden Pattern](../../wiki/concepts/knowledge-gardens.md)
   Score: 8.7765
   Snippet: xt pass.  ## Signals In This Demo  - &#91;&#91;Agentic Workflows&#93;&#93; show why durable artifacts matter - &#91;&#91;Knowledge Gardens&#93;&#93; explains why Markdown and backlinks are the right substrate - &#91;&#91;Evaluation Loops&#93;&#93; turns maintenance into an explicit workflow

4. [Evaluation Loops](../../wiki/sources/evaluation-loops.md)
   Score: 6.0273
   Snippet: # Evaluation Loops  This source emphasizes that a knowledge garden needs maintenance passes, not just data ingestion. Health checks, missing-data detection, and follow-up tasks make the corpus more useful

5. [Agentic Workflows](../../wiki/sources/agentic-workflows.md)
   Score: 5.1364
   Snippet: # Agentic Workflows  This source argues that LLM workflows become more reliable when they externalize their thinking into inspectable artifacts instead of hiding everything inside context windows.  ## H

## Suggested Workflow

- Use the prompt packet in `prompts/` to hand the question to an external LLM.
- File the resulting answer into `wiki/queries/` or `outputs/reports/`.
- Re-run `cognisync lint` and `cognisync plan` after incorporating new findings.
