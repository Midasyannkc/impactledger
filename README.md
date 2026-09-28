# ImpactLedger

A nonprofit-sector financial & donor data warehouse — real IRS Form 990
filings for 8 major US nonprofits, a medallion lakehouse (Bronze →
Bronze_Cleaned → Silver → Gold), a real-time donation-event streaming
lane alongside the daily batch warehouse, dbt-native SCD2 history, PII
masking + RBAC, a donor lapse-risk ML model with drift monitoring, and
Airflow orchestration — built to mirror the kind of engagement a data
consultancy serving nonprofit clients (world's-largest-nonprofits to
Fortune 500) would actually run.

This README is written as an incident report: what was built, what broke
along the way, how each failure was actually reproduced and fixed with
real measured evidence (not described or asserted), and what it costs.

**[Executive Summary (PPTX)](docs/ImpactLedger_Executive_Summary.pptx)**

## Why this project exists

This repo was built directly against a real Senior Data Engineer job
posting (Velir) that named specific tools and responsibilities: Spark,
dbt, Snowflake/BigQuery-class warehousing, real-time/micro-batch
processing, data quality assurance, Terraform-managed infrastructure, and
mentoring/cross-team collaboration. `data/SOURCE_DISCLOSURE.md` documents
exactly what data is real and what is disclosed-synthetic and why; every
other technical claim in this README and in `docs/incidents/` was
actually run, not just described.

## Architecture

```
Bronze (real IRS Form 990 JSON, via ProPublica API)
   |
   v  [Spark: schema cast, null/dedup filtering, manifest reconciliation]
Bronze_Cleaned (Parquet)
   |
   v  [dbt on DuckDB, standing in for a Terraform-managed Snowflake target]
Silver (stg_990_filing, dim_org [+ SCD2 snapshot], fact_990_filing [incremental],
        fact_donation_event, stg_donor_pii)
   |
   v
Gold (mart_fundraising_efficiency, mart_donor_channel_performance, dim_donor_masked)

                     donation events also flow through a parallel lane:
                     Spark Structured Streaming -> rolling near-real-time aggregate
```

Orchestrated end to end by a real Apache Airflow 3 DAG
(`orchestration/dags/impactledger_pipeline_dag.py`) — see Orchestration
below.

## Incidents

Each of these was reproduced for real (a real failure, with real
measured evidence) and fixed for real (re-verified, also with real
measured evidence). Full write-up and raw evidence file linked per
incident.

| # | Incident | JD line it addresses | Real result |
|---|---|---|---|
| 1 | [Low-latency donation reporting](docs/incidents/incident1_low_latency_reporting.md) | Real-time/micro-batch processing, low latency | Naive batch-only bound: up to 24h (avg 12h). Real Spark Structured Streaming lane, measured: **28.32s** end-to-end, 0 loss across 1,800 events. |
| 2 | [Entity resolution / data quality](docs/incidents/incident2_entity_resolution.md) | Data quality assurance | Naive exact-name join orphaned **7 of 8** real orgs against a realistic partner export. 3-tier EIN→normalized-name→token-subset match: **8/8** resolved, 0 false positives. |
| 3 | [Spark scalability / data skew](docs/incidents/incident3_spark_skew.md) | Builds/manages scalable Spark pipelines | Naive repartition-by-key: real **63.81x** partition skew ratio (AQE off). Salted two-stage aggregation: **18.58x** — reported honestly alongside a local-mode wall-time trade-off. |
| 4 | [dbt warehousing at scale](docs/incidents/incident4_dbt_warehousing_at_scale.md) | dbt at enterprise scale | Converted a full-refresh table to an incremental model + added a dbt-native SCD2 snapshot. Verified against a real second extract: incremental model picked up exactly the 1 new row (103→104); SCD2 correctly preserved both the old and new org_state with real validity timestamps. |
| 5 | [Data security (PII masking + RBAC)](docs/incidents/incident5_data_security.md) | Data governance/security | Added a disclosed-synthetic donor PII dimension; column-level masking (dbt macro, Snowflake-masking-policy stand-in) + documented RBAC grants. Verified with two real dbt runs under different roles: masked by default, unmasked only for the privileged role. |
| 6 (bonus) | [MLOps: donor lapse-risk model + drift](docs/incidents/incident6_mlops_drift.md) | (Beyond JD — MLOps maturity) | Real sklearn model + real PSI drift statistic (**0.3271**, threshold 0.2) caught a real distribution shift immediately — reported honestly that accuracy alone did *not* reliably catch it at this holdout size. |
| 7 | [Orchestration (Apache Airflow)](docs/incidents/incident7_orchestration.md) | Pipeline orchestration/scheduling | Real local Airflow 3 instance, real DAG, `airflow dags test`: all 4 tasks ran in dependency order, DagRun finished in **success** state, 23.13s real run duration. |

## Business solution

The gold-layer marts answer the questions a nonprofit's finance and
development teams actually ask: is fundraising efficiency improving
year over year (`mart_fundraising_efficiency`), and which donor channels
are performing (`mart_donor_channel_performance`) — with the latter
refreshed near-real-time via the streaming lane rather than waiting on
the next batch run. See `docs/STAKEHOLDER_ONE_PAGER.md` for the
non-technical version of this section.

## Cost trade-offs

- **Streaming lane runs only where staleness has a real cost**
  (donor-channel activity), not everywhere. The annual financial-
  efficiency mart stays on the cheaper daily batch/dbt path since its
  source data (annual filings) only changes once a year — see Incident 1.
- **Incremental dbt model + `delete+insert` strategy** over a full
  rebuild: pays only for genuinely new/changed `(org_ein, tax_year)`
  rows, not the whole filing history, every run — see Incident 4 and
  ADR-0002 for why `delete+insert` specifically (correctness on
  re-filed/amended years) over the cheaper `append`.
- **Spark salting is gated, not unconditional** — it costs one extra
  shuffle stage, worth paying only when a real skew check flags it. See
  Incident 3 and ADR-0003 for the honest local-vs-cluster trade-off this
  decision rests on.
- **PII masking at the model layer** costs one extra materialized table
  per masked entity, in exchange for exactly one auditable place PII
  exposure is decided rather than trusting every downstream query. See
  ADR-0004.
- **DuckDB locally, Snowflake documented** (ADR-0001) — every dbt result
  in this repo is real and reproducible without needing live cloud
  credentials; the SQL is written to be portable to a real Snowflake
  target via a profile swap.

## Repository layout

```
data/                    real IRS 990 data + disclosed-synthetic data, SOURCE_DISCLOSURE.md
ingestion/                bronze parsing, Spark Bronze_Cleaned layer, donor PII/data generators
dbt_impactledger/         silver/gold models, the dim_org SCD2 snapshot, the mask_pii macro
streaming/                real Spark Structured Streaming donation-event lane
scripts/incidents/        one reproduction script per incident
scripts/mlops/            donor lapse-risk model training + drift detection
orchestration/dags/       the real Airflow DAG
docs/incidents/           one write-up + one real evidence file per incident
docs/adr/                 architecture decision records
docs/ONBOARDING.md        setup/run instructions, including every real error hit and its fix
docs/STAKEHOLDER_ONE_PAGER.md   non-technical summary
docs/ImpactLedger_Executive_Summary.pptx   standalone executive summary deck
```

## Getting started

See `docs/ONBOARDING.md` for full setup and run instructions.

## Author

Christian Kouadio Kouassi
