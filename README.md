# MaritimeIQ

**Winner, Data + AI Track, SOLIXEmpower 2026.**

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

## Why

Shipping companies hold large volumes of operational data but can't point an LLM at it. The data
sits in silos and mixed formats (AIS positions in CSV, port calls in JSON, bills of lading in XML and
EDI, crew manifests in Excel, schedules in fixed-width). One vessel shows up as an IMO, an MMSI, a
call sign, and a name that changes after a sale. Codes are cryptic, and crew and customer records
carry PII that legal won't let an agent read raw. MaritimeIQ makes that data usable by an LLM:
correctly understood, correctly joined, and governed, with a human in the loop wherever the agents
are unsure.

## Results

From `make eval` (run 2026-10-29), scored against `data/hackathon/ground_truth/`. The source data is
**synthetic**, generated with planted errors (duplicate and misspelled vessels, one vessel split
across two MMSIs, unresolvable port codes, invalid IMOs) so every agent can be scored. Ground truth is
used only for evaluation, never as pipeline input.

| Area | Metric | Result |
|---|---|---|
| Entity resolution | Vessel F1 (by IMO), 120 vessel masters | **1.00** (precision 1.00, recall 1.00) |
| Entity resolution | Port recall (UN/LOCODE) | **1.00** |
| Entity resolution | Customer pairwise F1, 607 records | 0.80 (precision 0.69, recall 0.95) |
| Governance | PII recall, 15 PII columns | **93%** (14 of 15) |
| Governance | Adversarial guardrail probes blocked | **5 of 5** |
| Ask | Out-of-scope refusal | 75% (3 of 4) |
| Ask | Text-to-SQL accuracy, 12 benchmark questions | 33% (4 of 12; all 12 answered from Gold) |
| Semantics | Semantic accuracy, 128 scored columns | 31% |
| Semantics | LLM-as-Judge agreement with ground truth | 42% |

### Known limitations

- **The judge over-approves.** Of 76 proposals the Judge marked correct, 55 were wrong per ground
  truth; it flagged 33 of 88 wrong proposals. As built it does not yet filter bad semantic proposals,
  so fewer gaps reach human review than should. Next step: a stricter rubric with calibrated
  thresholds, and judging from richer column profiles.
- **Semantic accuracy is low** because of the point above: weak proposals pass the Judge and flow
  into Silver and Gold.
- **Text-to-SQL scores 4 of 12.** Every question was answered from Gold, but most answers didn't match
  ground truth, which tracks the semantic errors upstream.
- **One out-of-scope question was answered instead of refused** (fuel consumption per nautical mile,
  which the data does not support).
- The guardrail and refusal probe sets are small (5 and 4 cases).
- Built in one day for a hackathon; runs on local Spark only, not deployed.

## Quick start

Requires **Java 17**, **Python 3.10**, and **Node 18+**.

```bash
make install
cp .env.example .env   # set OPENAI_API_KEY
make ui                # build the web app and serve UI + API on http://127.0.0.1:8000
make demo              # unattended run (agent suggestions stand in for HITL) + eval
make eval              # score against ground truth
make test
```

For front-end development run `make api` and `make web` side by side, then open http://127.0.0.1:5173
(Vite hot reload, `/api` proxied to the FastAPI server).

Without an API key the agents fall back to deterministic heuristics, and Ask only answers questions
that match a discovered finding.

The source pack lives in `data/hackathon/` and is not committed.

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

`make eval` scores against `data/hackathon/ground_truth/`:

- semantic accuracy and PII recall against the data dictionary
- **LLM-as-Judge agreement**: whether the Judge's verdict matched the ground truth for each column
- entity resolution: vessel F1 (by IMO), customer pairwise F1, port recall
- Text-to-SQL accuracy on the 12 benchmark questions, run through the real `ask()`
- refusal rate on out-of-scope questions, and guardrail blocks

## Governance

Every agent tool call goes through a deterministic guard (`configs/policy.yaml`): no DDL/DML, no
Bronze PII reads by Ask, Gold writes need an approval id, PII is masked with a salted hash in tool
responses, and everything is audit-logged to `lakehouse/context/audit_log.jsonl`.

## Stack

PySpark 3.5 and Delta Lake 3.2 (medallion lakehouse), spark-xml, OpenAI GPT-4o-mini with Pydantic
structured outputs, rapidfuzz (entity matching), FastAPI with SSE, React + Vite + TypeScript, pytest.

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