# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Phase 0 — Setup & Alignment

- Initialize repository with README, LICENSE, .gitignore
- Set up GitHub issue templates, PR template, CI placeholder
- Create project directory structure (data, dbt_project, ml, docs, scripts)
- Add `pyproject.toml` with pinned dependencies
- Add `Makefile` with pipeline targets
- Initialize dbt project with DuckDB adapter
- Configure dbt materializations per layer
- Declare all 8 Olist source tables
- Add `scripts/setup_env.sh` and `scripts/download_data.sh`
- Document business definitions (`docs/definitions.md`)
- Document ML design (`docs/ml_design.md`)
- Document metric definitions (`docs/metrics.md`)
- Add runbook (`docs/runbook.md`)
