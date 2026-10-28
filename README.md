# MaritimeIQ

Agentic data activation for shipping: a **gated walkthrough** from messy pre-Bronze sources to AI-ready **Gold** on Spark + Delta Lake — LangGraph orchestration, MCP-governed tools, LLM-as-Judge, human-in-the-loop on every gap, an investigation agent that builds the Gold KPI mart, and guarded Text-to-SQL.

```
Raw siloed sources ──► Ingestion ──► Bronze (Delta, lineage)
                                       │
               Context agent + Judge ──► Semantic layer ◄── HITL: semantic gaps
                                       │
                  Entity resolution ──► Masters (vessel/port/customer) ◄── HITL: uncertain matches
                                       │
                     Transformation ──► Silver (UTC, unified codes, PII masked, QC)
                                       │
          Gold investigation + Judge ──► Gold plan ◄── HITL: human approval
                                       │
                                       ▼
                              Gold mart (dims, facts, KPIs, findings)
                                       │
                  Insight (Text-to-SQL) ──► Gold → Silver+semantics → refuse
```

## Quick start

Requires **Java 17 or 21** (`JAVA_HOME` set) and **Python 3.10/3.11**, plus the source pack in `data/hackathon/` (not in git).

```bash
make install
cp .env.example .env   # optional: OPENAI_API_KEY (and MARITIMEIQ_JUDGE_MODEL for a separate judge)
make ui                # walkthrough console (HITL path)
make demo              # unattended batch rebuild + eval
make test
```

Without a key, deterministic heuristics run everywhere; with a key, Context, Judge, Gold investigation and Ask use the LLM first.

## Walkthrough console (`make ui`)

Each **Continue** runs exactly one stage; progress is checkpointed in `lakehouse/context/pipeline_progress.json`, so nothing is re-run unless something upstream changed.

| Step | Continue runs | Gate before moving on |
|------|---------------|-----------------------|
| Sources | Bronze ingestion | — |
| Bronze | Context agent + LLM-as-Judge | — |
| Semantics / HITL Gaps | Entity resolution | every semantic gap approved / edited / rejected (PII flag editable) |
| Connect | Silver transform | every uncertain port / vessel / customer match decided |
| Silver | Gold investigation | readiness checklist (semantics coverage, no open reviews, QC passes) |
| Gold Build | — | **Approve & Build Gold** (human approver name) |
| Ask | — | Gold built |
| Trust | always available | adversarial probes, SQL guard probe, eval scorecards, audit log |

Human decisions invalidate downstream stages: a semantic edit rebuilds Silver and Gold; an entity decision re-runs ER first. Human corrections are reused for later sources (fewer reviews on source 2).

## Batch mode (`make demo`)

`scripts/run_demo.py` rebuilds everything unattended under an explicit, recorded policy: semantic gaps not graded *wrong* are approved by `batch-policy`, uncertain entity matches stay separate, unknown ports stay unmapped, and Gold is approved as `batch-cli --auto-approve-gold`. Flags: `--no-gold` (stop at the plan), `--resume`, `--no-eval`, `--no-baseline`.

## Agents

| Agent | Role |
|-------|------|
| Orchestrator | LangGraph `StateGraph`; resumable stages; HITL / approval gates |
| Ingestion | Multi-format sources → Bronze with lineage; unknown folders go through a generic loader |
| Context | LLM-first semantics (meaning, type, PII, code maps, joins); reuses human corrections |
| Judge | Grades semantics from profile evidence, Gold findings, and Ask answers; never sees ground truth |
| Entity resolution | IMO/MMSI/call-sign vessels with rename detection, reference-driven ports, tax-id + core-name customers; uncertain cases go to HITL |
| Transformation | UTC timestamps (port offsets learned from data), unified event codes across feeds, TEU, masters joined, semantic-layer PII masking, QC |
| Gold builder | Investigates Silver: facts, KPI library, LLM-proposed findings (SQL-guarded, judge-graded); writes only with a human approval id |
| Insight | Text-to-SQL cascade with guardrails (below) |

## Ask guardrails

1. Prompt guard: PII value requests, destructive intent, prompt injection.
2. Domain scope: questions outside the data's vocabulary are refused.
3. Verified Gold KPI queries (strict whole-word intents) → LLM SQL on Gold → LLM SQL on Silver + semantics.
4. `sqlglot` validation: exactly one SELECT, allowlisted views only, no file paths, table functions, `reflect`/`java_method`, or DDL/DML.
5. LLM-as-Judge: a result that does not answer the question is refused, not shown.
6. Every returned row is PII-masked.

Out-of-scope questions get: *"This question cannot be answered from the activated data."*

## Governance

- `configs/policy.yaml`: blocked SQL, PII patterns / terms, approval prefix, audit path.
- Approvals can only be issued by a human (`review.get_approval(agent="human")`) and persist across restarts.
- PII hashing salt comes from `MARITIMEIQ_PII_SALT`, else a random per-install salt in `lakehouse/.pii_salt` (gitignored).
- Audit log: `lakehouse/context/audit_log.jsonl`.

## Evaluation (`make eval`)

Scored against `data/hackathon/ground_truth/` (eval-only; the pipeline never reads it):

- semantic accuracy + coverage, PII recall, and a leak scan of real ground-truth PII values in Silver/Gold
- judge agreement with ground truth
- vessel F1, customer pairwise F1, port recall
- NL accuracy on the 12 benchmark questions: every answer value must appear within 3% in the same row as its keys
- Bronze baseline: same LLM on raw Bronze tables (needs a key)
- out-of-scope refusal rate; adversarial prompts and SQL attacks run end to end

Results: `.eval_results/latest.md` / `latest.json`, also shown in Trust.

## Project layout

```
src/maritimeiq/
  agents/      orchestrator, ingestion, context, judge, entity_resolution, transform, gold_builder, insight, llm
  governance/  guard.py (policy, PII, approvals, audit), sql_guard.py (sqlglot validation)
  mcp/         guarded tool gateway (lakehouse / reference / review)
  utils/       paths, env, ports (reference-driven port resolver), feed
  eval/        harness
  ui/          walkthrough console
configs/       spark.yaml, policy.yaml, prompts/
deploy/        Dockerfile, Daytona, Cloud Run notes
lakehouse/     bronze / silver / gold / context (generated)
```

## Known data caveats

- AIS history thins after mid-2022; vessel identity is IMO-centric.
- Port-agent events whose port is still an open HITL alias (e.g. "Europoort") are excluded until mapped, which biases some European waits low.
- Benchmark set has 12 questions; ground truth is split across files in `ground_truth/`.
