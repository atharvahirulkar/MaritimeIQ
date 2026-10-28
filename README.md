# MaritimeIQ

Agentic data activation for shipping. Messy feeds land in **Bronze**; a set of agents, guided by a
human where they are unsure, turn them into a clean, documented, connected **Silver** layer; a
discovery agent then investigates Silver for the metrics that matter and builds an AI-ready **Gold**
mart that guarded Text-to-SQL answers questions from.

```
sources ─► Ingestion ─► Bronze ─► Context + LLM-as-Judge ─► Quality scan
                                         │                       │
                                         └──► HITL: field meanings, port/code gaps
                                                                 │
           Entity resolution (vessels, ports, customers) ◄───────┘
                     │ └──► HITL: uncertain entity links
                     ▼
           Transform ─► Silver (clean, PII-masked) + semantic layer + relationships
                     ▼
           Discovery ─► ranked findings ─► Gold builder ─► dims · facts · aggregates · wide view
                                                                 ▼
           Ask: Gold → Silver + semantics → "This question cannot be answered from the activated data."
```

## Quick start

Requires **Java 17**, **Python 3.10**, and **Node 18+**.

```bash
make install
cp .env.example .env   # set OPENAI_API_KEY
make ui                # build the web app and serve UI + API on http://127.0.0.1:8000
make demo              # unattended run (agent suggestions stand in for HITL) + eval
make test
```

For front-end development run `make api` and `make web` side by side, then open http://127.0.0.1:5173
(Vite hot reload, `/api` proxied to the FastAPI server).

Without an API key the agents fall back to deterministic heuristics, and Ask only answers questions
that match a discovered finding.

## The walkthrough

A React single-page app (`web/`) on top of a FastAPI service (`src/maritimeiq/api/`). Every step has
its own URL, so refresh and back/forward resume where you were. Each step shows what its agent
produced. **Continue** runs the next stage as a background job, only when its output is missing or
stale (an upstream stage re-ran, or you made new HITL decisions); otherwise it just moves on. While a
stage runs, the page stays usable and the Agent Feed streams progress live.

| Step | What happens |
|---|---|
| Sources | The raw pack in `data/hackathon/sources/` (CSV, JSON, XML, fixed-width, EDI, Excel). Read-only. |
| Bronze | Ingestion lands every feed as a raw Delta table with lineage. |
| Semantics | The Context agent proposes meaning, type, PII class, code values, and join hints per column; the LLM-as-Judge grades each proposal from masked profiles. Low scores become HITL gaps. |
| Quality | Duplicates, sparse columns, port values that don't resolve to a UN/LOCODE, unmapped event codes, invalid IMOs. |
| HITL Gaps | One table for field-meaning and data gaps: Approve the agent's suggestion, Reject, or Edit with your own value. Decisions are stored as mappings and replayed on every run. Continue is blocked until the queue is clear. |
| Connect | Entity resolution builds vessel, port, and customer masters. Uncertain links go to you. |
| Silver | Cleanup report (what was fixed, what is still open), Silver tables with column docs, and the relationship graph. |
| Insights | The Discovery agent runs KPI recipes the data supports plus LLM-proposed metrics, then ranks them. Pick which become Gold. |
| Gold | Dimensions, facts, a wide port-call view, and one aggregate per selected finding, with a catalog of columns and join keys. |
| Ask | Guarded Text-to-SQL: adversarial guard → LLM plan (may declare the question unsupported) → table allowlist → Spark analysis with one repair → SQL guard → Judge check. Gold is tried first, then Silver; otherwise a refusal. |
| Trust | Judge verdicts per table, guardrail probe, eval scores, audit trail. |

**Hard Reset** keeps `data/` and Bronze and moves Silver, Gold, and all context into
`lakehouse/_reset_backup_<timestamp>/`, so the walkthrough restarts at Bronze without losing anything.

## Evaluation

`make eval` scores against `data/hackathon/ground_truth/`, which is used only for evaluation and never as pipeline input:

- semantic accuracy and PII recall against the data dictionary
- **LLM-as-Judge agreement**: whether the Judge's verdict matched the ground truth for each column
- entity resolution: vessel F1 (by IMO), customer pairwise F1, port recall
- Text-to-SQL accuracy on the 12 benchmark questions, run through the real `ask()`
- refusal rate on out-of-scope questions, and guardrail blocks

## Governance

Every agent tool call goes through a deterministic guard (`configs/policy.yaml`): no DDL/DML, no
Bronze PII reads by Ask, Gold writes need an approval id, and everything is audit-logged.

## Layout

```
src/maritimeiq/
  agents/         orchestrator, ingestion, context, judge, quality, entity_resolution,
                  transform, discovery, gold_builder, insight, vocab, llm
  context_store/  semantic layer, proposals, gaps, mappings, corrections
  mcp/            guarded lakehouse + review tools
  governance/     policy guard
  eval/           harness
  walkthrough/    step gating, staleness, HITL queue, summaries (UI-agnostic)
  api/            FastAPI routes, stage job runner, SSE agent feed
web/              React + Vite + TypeScript walkthrough UI
configs/          spark.yaml, policy.yaml, prompts/
lakehouse/        bronze / silver / gold / context (generated)
```
