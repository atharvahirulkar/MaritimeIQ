# MaritimeIQ

Agentic data activation for shipping: a **gated walkthrough** from messy sources to AI-ready **Gold** on Spark + Delta Lake — MCP-governed tools, LLM-as-Judge, HITL on gaps, relational Gold mart, and guarded Text-to-SQL.

```
Raw siloed sources ──► Ingestion Agent ──► Bronze (Delta)
                              │
                     Context Agent (+ HITL) ──► Semantic layer
                              │
                   Entity Resolution (+ HITL) ──► Masters
                              │
                   Transformation Agent ──► Silver (masked)
                              │
                      Insight Agent (+ HITL Gold) ──► Answers / Gold
                              │
                         Eval harness ──► Proof
```

Every agent tool call passes a **deterministic governance guard** (no `DROP`/`DELETE`, Insight cannot read Bronze PII, Gold writes need approval).

## Quick start

Requires **Java 17** and **Python 3.10**.

```bash
make install
cp .env.example .env   # set OPENAI_API_KEY
make demo              # rebuild lakehouse + eval
make ui                # walkthrough console (Streamlit)
make test
```

Without a key, deterministic heuristics still run; with a key, Context/Judge/Ask use the LLM first.

## Walkthrough console

`make ui` opens a **step-by-step walkthrough** (Back / Continue), not a fleet dashboard:

| Region | What you see |
|--------|----------------|
| Status bar | Phase, LLM on/off, NL accuracy, guard blocks |
| Left rail | Gated steps · advanced batch run (optional) |
| Center | Current step content + **Back / Continue** |
| Right rail | Agent feed (`proposal_ready`, `hitl_waiting`, `guard_block`, …) |

### Click path (Act 1 → Act 2)

**Act 1 — Activate**

1. **Sources** — pre-Bronze files from `data/hackathon/sources/`.  
2. **Continue → Bronze** — ingest messy raw tables; inspect samples.  
3. **Continue → Semantics** — LLM meanings + LLM-as-Judge grades.  
4. **HITL Gaps** — approve/edit/reject (or accept high-confidence); pipeline pauses until clear.  
5. **Connect** — entity-resolution masters linking sources.  
6. **Silver** — governed tables; checklist must pass (**Silver ready**).

**Act 2 — Gold then Ask**

7. **Gold Build** — agent investigates Silver + semantics and materializes a **relational Gold mart** (dims, facts, `v_ops_overview`).  
8. **Ask** — Text-to-SQL cascade: **Gold first → Silver + semantics → refuse** with a simple message if out of scope (no sample-row fallback). Badge shows answer **layer**.  
9. **Trust** — adversarial probe + eval scorecards (always reachable).

CLI `make demo` still batch-builds the lakehouse with soft-approve semantics for eval.

## Data

- Pipeline input: [`data/hackathon/sources/`](data/hackathon/sources/)  
- Eval-only: [`data/hackathon/ground_truth/`](data/hackathon/ground_truth/) (never used as pipeline input)

## Architecture

| Component | Role |
|-----------|------|
| Orchestrator | Plans phases, HITL checkpoints |
| Ingestion | Multi-format → Bronze + lineage |
| Context | LLM-first semantics + corrections learning |
| Entity resolution | IMO/MMSI/tax-ID + rapidfuzz |
| Transformation | Unified Silver + salted-hash PII |
| Gold builder | Investigates Silver → relational Gold mart + catalog |
| Insight | Text-to-SQL cascade Gold → Silver+semantics → refuse |
| MCP gateway | `lakehouse` / `reference` / `review` |
| Guard | Policy YAML + audit log |
| Eval | Semantics, PII, ER F1, 12 NL Qs, 5 security cases |
| UI | Streamlit walkthrough (`src/maritimeiq/ui/`) |

## Cloud / Daytona (Phase 2)

See [`deploy/README.md`](deploy/README.md):

- **Dockerfile** — Java 17 + Python 3.10 image for Streamlit  
- **Daytona** — shareable workspace config  
- **GCS sync** — `scripts/sync_gcs.sh` uploads `lakehouse/` + `.eval_results/`  
- **Vertex** — set `MARITIMEIQ_LLM_PROVIDER=vertex` + OpenAI-compatible base URL  
- **Cloud Run** — recipe to serve prebuilt lakehouse UI  

GCP is optional (shareability / Vertex). Local Spark remains the default.

## Project layout

```
src/maritimeiq/
  agents/     orchestrator, ingestion, context, ER, transform, gold_builder, insight, judge, llm
  ui/         walkthrough console (theme, components, views, walkthrough.py)
  mcp/        guarded tool gateway
  governance/ policy guard
  spark/      local Spark + Delta
  eval/       harness
configs/      spark.yaml, policy.yaml, prompts/
deploy/       Dockerfile, Daytona, Cloud Run notes
lakehouse/    bronze / silver / gold / context (generated)
```

## Known data caveats

- AIS history coverage thins after mid-2022; renames use IMO-centric ER.  
- Benchmark set has **12** gold questions.  
- Ground truth is split files under `ground_truth/`.  
