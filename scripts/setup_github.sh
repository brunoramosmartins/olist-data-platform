#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================
REPO_NAME="${REPO_NAME:-olist-data-platform}"
GH_USER=$(gh api user --jq '.login')
REPO="${GH_USER}/${REPO_NAME}"

echo "============================================"
echo "  Olist Data Platform — GitHub Setup"
echo "  Repo: ${REPO}"
echo "============================================"

# ============================================================================
# 1. CREATE REPOSITORY
# ============================================================================
echo ""
echo ">>> Step 1: Creating repository..."

gh repo create "${REPO_NAME}" \
  --public \
  --description "End-to-end data platform: raw e-commerce data → reliable metrics → monitored delivery-delay predictions" \
  --clone

cd "${REPO_NAME}"

# Create initial files
cat > README.md << 'EOF'
# Olist Data Platform

End-to-end data system that transforms raw e-commerce data into reliable metrics and monitored delivery-delay predictions.

**Stack:** DuckDB · dbt-core · scikit-learn · Parquet · GitHub Actions

> 🚧 Under construction — see [Roadmap](docs/roadmap.md) for progress.
EOF

cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
.venv/
venv/
*.egg

# Data
data/raw/*.csv
data/ml/*.parquet
*.duckdb
*.duckdb.wal

# ML artifacts (large files)
ml/models/*.joblib
ml/models/*.pkl
ml/predictions/*.parquet
ml/data_snapshots/*.parquet

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
EOF

cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF

git add -A
git commit -m "chore: initialize repository"
git push -u origin main

echo "  ✅ Repository created and pushed"

# ============================================================================
# 2. CREATE ISSUE TEMPLATES AND PR TEMPLATE
# ============================================================================
echo ""
echo ">>> Step 2: Creating templates..."

mkdir -p .github/ISSUE_TEMPLATE
mkdir -p .github/workflows

cat > .github/ISSUE_TEMPLATE/task.md << 'TEMPLATE'
---
name: Task
about: A specific piece of work
labels: ''
---

## Context
<!-- Why does this issue exist? What problem does it solve? -->

## Tasks
<!-- Specific, actionable checklist -->
- [ ] 

## Definition of Done
<!-- Verifiable completion criteria -->
- [ ] 

## References
<!-- Relevant links, docs, related code -->
- 
TEMPLATE

cat > .github/ISSUE_TEMPLATE/bug.md << 'TEMPLATE'
---
name: Bug Report
about: Something is broken
labels: 'type: bug'
---

## Observed Behavior
<!-- What happened? -->

## Expected Behavior
<!-- What should have happened? -->

## Steps to Reproduce
1. 

## Environment
- dbt version:
- DuckDB version:
- Python version:
- OS:

## Additional Context
<!-- Logs, screenshots, error messages -->
TEMPLATE

cat > .github/PULL_REQUEST_TEMPLATE.md << 'TEMPLATE'
## Summary
<!-- What does this PR do? -->
Closes #

## Changes
- 

## Testing
- [ ] `dbt build --select <models>` passes
- [ ] All schema tests pass
- [ ] Manual spot-check completed

## Checklist
- [ ] Code follows project conventions
- [ ] Documentation updated
- [ ] No future-leaking data in feature models
- [ ] CHANGELOG updated (for tagged releases)
TEMPLATE

# Minimal CI placeholder
cat > .github/workflows/ci.yml << 'CIFILE'
name: CI

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Placeholder
        run: echo "CI pipeline — will be extended per phase"
CIFILE

git add -A
git commit -m "chore: add issue templates, PR template, and CI placeholder"
git push

echo "  ✅ Templates created"

# ============================================================================
# 3. BRANCH PROTECTION
# ============================================================================
echo ""
echo ">>> Step 3: Setting branch protection on main..."

gh api "repos/${REPO}/branches/main/protection" \
  --method PUT \
  --input - << 'JSON' 2>/dev/null || echo "  ⚠️  Branch protection requires GitHub Pro/Team for private repos. Skipping."
{
  "required_status_checks": {
    "strict": true,
    "contexts": []
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0
  },
  "restrictions": null
}
JSON

echo "  ✅ Branch protection configured"

# ============================================================================
# 4. CREATE LABELS
# ============================================================================
echo ""
echo ">>> Step 4: Creating labels..."

# Delete default labels
for label in "bug" "documentation" "duplicate" "enhancement" "good first issue" \
  "help wanted" "invalid" "question" "wontfix"; do
  gh label delete "${label}" --repo "${REPO}" --yes 2>/dev/null || true
done

# Type labels
gh label create "type: feature"   --color "0E8A16" --description "New functionality"           --repo "${REPO}" --force
gh label create "type: bug"       --color "D73A4A" --description "Something broken"             --repo "${REPO}" --force
gh label create "type: test"      --color "FBCA04" --description "Test additions"               --repo "${REPO}" --force
gh label create "type: docs"      --color "0075CA" --description "Documentation"                --repo "${REPO}" --force
gh label create "type: refactor"  --color "D4C5F9" --description "Code improvement"             --repo "${REPO}" --force
gh label create "type: chore"     --color "EDEDED" --description "Maintenance"                  --repo "${REPO}" --force

# Layer labels
gh label create "layer: infra"         --color "BFD4F2" --description "Repo, CI, environment"   --repo "${REPO}" --force
gh label create "layer: staging"       --color "C2E0C6" --description "Staging models"           --repo "${REPO}" --force
gh label create "layer: intermediate"  --color "FEF2C0" --description "Intermediate models"      --repo "${REPO}" --force
gh label create "layer: marts"         --color "F9D0C4" --description "Mart models"              --repo "${REPO}" --force
gh label create "layer: metrics"       --color "E6CCB2" --description "Metrics layer"            --repo "${REPO}" --force
gh label create "layer: features"      --color "D7BDE2" --description "Feature layer"            --repo "${REPO}" --force
gh label create "layer: ml"            --color "AED6F1" --description "ML pipeline"              --repo "${REPO}" --force
gh label create "layer: dbt"           --color "F5B041" --description "dbt-specific"             --repo "${REPO}" --force
gh label create "layer: business"      --color "EB984E" --description "Business logic"           --repo "${REPO}" --force

# Priority labels
gh label create "priority: critical"  --color "B60205" --description "Blocks other work"       --repo "${REPO}" --force
gh label create "priority: high"      --color "D93F0B" --description "Important"                --repo "${REPO}" --force
gh label create "priority: medium"    --color "FBCA04" --description "Normal priority"          --repo "${REPO}" --force
gh label create "priority: low"       --color "0E8A16" --description "Nice to have"             --repo "${REPO}" --force

# Phase labels
for i in $(seq 0 9); do
  PHASE_NAMES=("Setup" "Staging" "Intermediate" "Marts" "Metrics" "Features" "Training" "Inference" "Monitoring" "Retraining")
  gh label create "phase: ${i}" --color "C5DEF5" --description "Phase ${i} — ${PHASE_NAMES[$i]}" --repo "${REPO}" --force
done

echo "  ✅ Labels created"

# ============================================================================
# 5. CREATE MILESTONES
# ============================================================================
echo ""
echo ">>> Step 5: Creating milestones..."

declare -a MS_TITLES=(
  "Phase 0 — Setup & Alignment"
  "Phase 1 — Staging (Reliable Data)"
  "Phase 2 — Intermediate (Business Rules)"
  "Phase 3 — Marts (Analytical Model)"
  "Phase 4 — Metrics Layer"
  "Phase 5 — Feature Layer (Bridge to ML)"
  "Phase 6 — Model Training"
  "Phase 7 — Inference Pipeline"
  "Phase 8 — Monitoring (Feedback Loop)"
  "Phase 9 — Retraining (Continuous Cycle)"
)

declare -a MS_DESCS=(
  "Establish technical base and semantic scope"
  "Standardize and stabilize raw data"
  "Resolve domain complexity"
  "Create consumable analytical model"
  "Formalize business metric definitions"
  "Prepare data for ML modeling"
  "Build baseline ML model"
  "Simulate production inference"
  "Track model performance and drift"
  "Close the ML loop"
)

for i in $(seq 0 9); do
  gh api "repos/${REPO}/milestones" \
    --method POST \
    -f title="${MS_TITLES[$i]}" \
    -f description="${MS_DESCS[$i]}" \
    -f state="open" \
    > /dev/null
done

echo "  ✅ Milestones created"

# ============================================================================
# 6. CREATE ALL ISSUES
# ============================================================================
echo ""
echo ">>> Step 6: Creating issues..."

# Helper: get milestone number by title substring
get_milestone() {
  gh api "repos/${REPO}/milestones" --jq ".[] | select(.title | contains(\"Phase ${1}\")) | .number"
}

# --- PHASE 0 ISSUES ---
MS0=$(get_milestone "0")

gh issue create --repo "${REPO}" \
  --title "feat: initialize repository and CI scaffold" \
  --milestone "${MS0}" \
  --label "type: feature,layer: infra,priority: high,phase: 0" \
  --body '## Context
The project needs a clean repository foundation before any data work begins. This includes branch protection, a CI stub, and the label/milestone taxonomy.

## Tasks
- [ ] Create repo with README.md, .gitignore, LICENSE
- [ ] Configure branch protection on main: require PR, require status check
- [ ] Create milestones for Phase 0 through Phase 9
- [ ] Create all labels (see workflow standards)
- [ ] Add minimal CI workflow with placeholder

## Definition of Done
- [ ] Direct push to main is blocked
- [ ] All 10 milestones exist
- [ ] All labels are created
- [ ] CI runs on every PR

## References
- [GitHub branch protection docs](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-a-branch-protection-rule)'

gh issue create --repo "${REPO}" \
  --title "feat: set up Python environment and Makefile" \
  --milestone "${MS0}" \
  --label "type: feature,layer: infra,priority: high,phase: 0" \
  --body '## Context
Reproducible environment setup and a single entry point for all pipeline operations.

## Tasks
- [ ] Create pyproject.toml with pinned dependencies (dbt-duckdb, duckdb, scikit-learn, pandas, pyarrow)
- [ ] Create Makefile with targets: install, dbt-build, dbt-test, ml-train, ml-predict, pipeline
- [ ] Create scripts/setup_env.sh
- [ ] Verify `make install && dbt debug` exits 0

## Definition of Done
- [ ] `make install` works in a fresh venv
- [ ] `dbt debug` passes
- [ ] Makefile has at least 6 targets

## References
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)'

gh issue create --repo "${REPO}" \
  --title "feat: initialize dbt project structure" \
  --milestone "${MS0}" \
  --label "type: feature,layer: dbt,priority: high,phase: 0" \
  --body '## Context
Proper dbt project configuration prevents rework. Materialization strategy, package management, and directory structure are set once.

## Tasks
- [ ] Run `dbt init` and configure dbt_project.yml
- [ ] Create profiles.yml with DuckDB target
- [ ] Add packages.yml (dbt-utils, dbt-expectations), run `dbt deps`
- [ ] Configure materialization defaults per layer (staging=view, intermediate=view, marts=table, metrics=table, features=table)
- [ ] Create empty model directories

## Definition of Done
- [ ] `dbt deps` succeeds
- [ ] `dbt debug` succeeds
- [ ] Directory structure matches spec

## References
- [dbt project structure guide](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)'

gh issue create --repo "${REPO}" \
  --title "feat: ingest Olist dataset and declare sources" \
  --milestone "${MS0}" \
  --label "type: feature,layer: staging,priority: high,phase: 0" \
  --body '## Context
The Olist dataset (8 CSVs from Kaggle) is the raw input. Automated download and dbt source declarations ensure consistency.

## Tasks
- [ ] Write scripts/download_data.sh
- [ ] Create _stg__sources.yml declaring all 8 source tables
- [ ] Run `dbt compile` to verify source refs
- [ ] Add data directories to .gitignore
- [ ] Document download instructions in README.md

## Definition of Done
- [ ] Script downloads and extracts all 8 CSVs
- [ ] `dbt compile` succeeds
- [ ] Data files are gitignored

## References
- [Olist dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)'

gh issue create --repo "${REPO}" \
  --title "docs: document business definitions and ML problem scope" \
  --milestone "${MS0}" \
  --label "type: docs,layer: business,priority: critical,phase: 0" \
  --body '## Context
Every subsequent phase depends on shared definitions. Ambiguity here causes cascading bugs.

## Tasks
- [ ] Write docs/definitions.md with:
  - Valid order: order_status in (delivered, shipped, invoiced, processing) AND at least one order_item
  - Delay: delivered order where order_delivered_customer_date > order_estimated_delivery_date
  - Delay amount: datediff(day, estimated, actual). Positive = late.
  - ML scope: binary classification, predict is_delayed at order creation time
- [ ] Write docs/ml_design.md with problem statement, target, feature constraints, evaluation plan
- [ ] Self-review: no definition uses "maybe", "usually", or "it depends"

## Definition of Done
- [ ] Both docs committed
- [ ] All rules are deterministic
- [ ] ML problem boundary is explicit

## References
- Olist dataset documentation on Kaggle'

# --- PHASE 1 ISSUES ---
MS1=$(get_milestone "1")

gh issue create --repo "${REPO}" \
  --title "feat: create stg_orders staging model" \
  --milestone "${MS1}" \
  --label "type: feature,layer: staging,priority: high,phase: 1" \
  --body '## Context
stg_orders is the most referenced model. Correct typing here prevents cascading bugs.

## Tasks
- [ ] Create models/staging/stg_orders.sql
- [ ] Rename all columns to snake_case
- [ ] Cast timestamp strings to TIMESTAMP
- [ ] Add column descriptions in _stg__models.yml
- [ ] Add tests: PK not_null/unique, accepted_values on order_status

## Definition of Done
- [ ] `dbt build --select stg_orders` passes
- [ ] All columns properly typed
- [ ] Test coverage on PK and status

## References
- docs/definitions.md'

gh issue create --repo "${REPO}" \
  --title "feat: create remaining staging models" \
  --milestone "${MS1}" \
  --label "type: feature,layer: staging,priority: high,phase: 1" \
  --body '## Context
The remaining 7 staging models follow the same rename-cast-clean pattern.

## Tasks
- [ ] Create stg_order_items.sql with proper numeric casting
- [ ] Create stg_order_payments.sql with standardized payment_type
- [ ] Create stg_order_reviews.sql with integer review_score
- [ ] Create stg_customers.sql with trimmed city, standardized state
- [ ] Create stg_sellers.sql with trimmed city, standardized state
- [ ] Create stg_products.sql with null-safe product_category_name
- [ ] Create stg_geolocation.sql with deduplication logic
- [ ] Add schema tests to _stg__models.yml for all models
- [ ] Add relationship test: stg_order_items.order_id → stg_orders.order_id

## Definition of Done
- [ ] `dbt build --select staging` passes with 0 failures
- [ ] Every model has PK tests
- [ ] All columns have descriptions

## References
- Kaggle dataset schema'

gh issue create --repo "${REPO}" \
  --title "test: add custom singular tests for staging" \
  --milestone "${MS1}" \
  --label "type: test,layer: staging,priority: medium,phase: 1" \
  --body '## Context
Domain-specific anomalies that schema tests cannot express.

## Tasks
- [ ] Create tests/assert_no_future_orders.sql
- [ ] Validate test returns 0 rows
- [ ] Document test purpose with comment

## Definition of Done
- [ ] Test passes
- [ ] Comment explains the business rule

## References
- [dbt singular tests](https://docs.getdbt.com/docs/build/data-tests#singular-data-tests)'

# --- PHASE 2 ISSUES ---
MS2=$(get_milestone "2")

gh issue create --repo "${REPO}" \
  --title "feat: aggregate payments per order" \
  --milestone "${MS2}" \
  --label "type: feature,layer: intermediate,priority: high,phase: 2" \
  --body '## Context
One order can have multiple payment rows. Without aggregation, every downstream join creates duplicates.

## Tasks
- [ ] Create models/intermediate/int_payments_aggregated.sql
- [ ] Group by order_id: total_payment_value (sum), payment_count, payment_type_list (string_agg), max_installments
- [ ] Handle edge case: payment_value = 0
- [ ] Add schema tests in _int__models.yml

## Definition of Done
- [ ] One row per order_id
- [ ] SUM(total_payment_value) matches raw payments total
- [ ] Tests pass: unique and not_null on order_id

## References
- Olist dataset: "each order can have multiple payment types"'

gh issue create --repo "${REPO}" \
  --title "feat: normalize order status" \
  --milestone "${MS2}" \
  --label "type: feature,layer: intermediate,priority: medium,phase: 2" \
  --body '## Context
Raw statuses need simplification for downstream analysis.

## Tasks
- [ ] Create models/intermediate/int_order_status_normalized.sql
- [ ] Map raw statuses to: placed, in_transit, delivered, canceled, other
- [ ] Add is_delivered and is_canceled boolean flags
- [ ] Document mapping in CTE comment
- [ ] Add accepted_values test on normalized status

## Definition of Done
- [ ] Every raw status maps to exactly one normalized status
- [ ] Mapping documented in CTE comment
- [ ] is_delivered and is_canceled are mutually exclusive

## References
- docs/definitions.md'

gh issue create --repo "${REPO}" \
  --title "feat: build int_orders_enriched with delay calculation" \
  --milestone "${MS2}" \
  --label "type: feature,layer: intermediate,priority: critical,phase: 2" \
  --body '## Context
Keystone model of the intermediate layer. Joins orders, payments, status, and reviews into a single enriched row per order. Computes delay metrics.

## Tasks
- [ ] Create models/intermediate/int_orders_enriched.sql
- [ ] Join: stg_orders + int_payments_aggregated + int_order_status_normalized
- [ ] Left join stg_order_reviews (deduplicate: most recent per order)
- [ ] Calculate: delivery_days, estimated_delivery_days, delay_days, is_delayed
- [ ] Handle nulls: is_delayed = NULL when delivery date is null
- [ ] Add schema tests

## Definition of Done
- [ ] One row per order_id
- [ ] is_delayed = 1 iff delay_days > 0
- [ ] Row count matches stg_orders
- [ ] Tests pass

## References
- docs/definitions.md for delay formula'

# --- PHASE 3 ISSUES ---
MS3=$(get_milestone "3")

gh issue create --repo "${REPO}" \
  --title "feat: create fact_orders mart (order grain)" \
  --milestone "${MS3}" \
  --label "type: feature,layer: marts,priority: critical,phase: 3" \
  --body '## Context
Central fact table. Grain decision is CLOSED: 1 row per order. Item-level analysis uses fact_order_items.

## Tasks
- [ ] Create models/marts/fact_orders.sql
- [ ] Grain: 1 row per order
- [ ] Aggregate from stg_order_items: item_count, total_freight_value, primary product_category, primary seller_id
- [ ] Include measures: total_payment_value, delivery_days, delay_days, is_delayed, review_score
- [ ] Include FK: customer_key
- [ ] Include dates: order_date, delivery_date, estimated_delivery_date
- [ ] Materialize as table

## Definition of Done
- [ ] Exactly 1 row per order_id
- [ ] Grain documented in model description
- [ ] `dbt build --select fact_orders` passes

## References
- int_orders_enriched, Kimball methodology'

gh issue create --repo "${REPO}" \
  --title "feat: create fact_order_items mart (item grain)" \
  --milestone "${MS3}" \
  --label "type: feature,layer: marts,priority: high,phase: 3" \
  --body '## Context
Some analyses need item-level granularity. This fact table complements fact_orders.

## Tasks
- [ ] Create models/marts/fact_order_items.sql
- [ ] Grain: 1 row per order-item (composite key: order_id + order_item_id)
- [ ] Include FK to dim_products, dim_sellers
- [ ] Add PK test on composite key

## Definition of Done
- [ ] Composite PK is unique and not null
- [ ] FK relationships pass
- [ ] Row count matches stg_order_items

## References
- stg_order_items'

gh issue create --repo "${REPO}" \
  --title "feat: create dimension tables (customers, sellers, products)" \
  --milestone "${MS3}" \
  --label "type: feature,layer: marts,priority: high,phase: 3" \
  --body '## Context
Dimension tables provide descriptive attributes for fact table analysis.

## Tasks
- [ ] dim_customers.sql — deduplicate by customer_unique_id
- [ ] dim_sellers.sql — select from staging
- [ ] dim_products.sql — coalesce null categories to uncategorized
- [ ] PK tests (unique, not_null) for all three
- [ ] Relationship tests from fact_orders to each dimension
- [ ] Column descriptions in _marts__models.yml

## Definition of Done
- [ ] Each dimension has one row per business key
- [ ] All tests pass
- [ ] All columns documented

## References
- Staging models'

# --- PHASE 4 ISSUES ---
MS4=$(get_milestone "4")

gh issue create --repo "${REPO}" \
  --title "feat: implement daily GMV metric" \
  --milestone "${MS4}" \
  --label "type: feature,layer: metrics,priority: high,phase: 4" \
  --body '## Context
GMV is the top-line metric. Single definition prevents conflicting numbers.

## Tasks
- [ ] Create models/metrics/met_daily_gmv.sql
- [ ] Filter to valid orders per docs/definitions.md
- [ ] Aggregate total_payment_value by order_date
- [ ] Add not_null test on order_date and gmv
- [ ] Document formula in docs/metrics.md

## Definition of Done
- [ ] One row per date
- [ ] SUM(gmv) matches fact_orders total for valid orders
- [ ] Formula documented in docs/metrics.md

## References
- fact_orders, docs/definitions.md'

gh issue create --repo "${REPO}" \
  --title "feat: implement daily revenue and delay rate metrics" \
  --milestone "${MS4}" \
  --label "type: feature,layer: metrics,priority: high,phase: 4" \
  --body '## Context
Revenue and delay rate complete the core metrics triad.

## Tasks
- [ ] Create met_daily_revenue.sql — document revenue formula decision
- [ ] Create met_daily_delay_rate.sql — use safe_divide macro
- [ ] Add schema tests for both
- [ ] Document both formulas in docs/metrics.md

## Definition of Done
- [ ] Both produce one row per date
- [ ] delay_rate is between 0 and 1
- [ ] Both documented in docs/metrics.md

## References
- fact_orders, macros/safe_divide.sql'

gh issue create --repo "${REPO}" \
  --title "feat: create reusable SQL macros (safe_divide, cents_to_currency)" \
  --milestone "${MS4}" \
  --label "type: feature,layer: dbt,priority: medium,phase: 4" \
  --body '## Context
Centralized macros prevent inconsistent implementations.

## Tasks
- [ ] Create macros/safe_divide.sql: returns NULL on zero denominator
- [ ] Create macros/cents_to_currency.sql (if applicable)
- [ ] Use in metrics models
- [ ] Add documentation as comments

## Definition of Done
- [ ] safe_divide(1, 0) returns NULL
- [ ] At least one model uses the macro

## References
- [dbt macros docs](https://docs.getdbt.com/docs/build/jinja-macros)'

# --- PHASE 5 ISSUES ---
MS5=$(get_milestone "5")

gh issue create --repo "${REPO}" \
  --title "feat: design and document feature set" \
  --milestone "${MS5}" \
  --label "type: feature,layer: ml,priority: critical,phase: 5" \
  --body '## Context
Feature engineering must balance richness with temporal safety. Every feature must be verifiable as known at order creation time.

## Tasks
- [ ] Review int_orders_enriched for candidates
- [ ] For each candidate, answer: "Is this known at order creation time?"
- [ ] List rejected features with reasons (e.g., delivery_days is post-purchase — leakage)
- [ ] Document final list in docs/ml_design.md with leakage risk table
- [ ] Define encoding strategy for categoricals

## Definition of Done
- [ ] Feature table in docs/ml_design.md with: name, source, type, leakage_safe (yes/no)
- [ ] All features marked leakage_safe=yes have justification

## References
- int_orders_enriched, docs/definitions.md'

gh issue create --repo "${REPO}" \
  --title "feat: build fct_order_features with window functions" \
  --milestone "${MS5}" \
  --label "type: feature,layer: features,priority: critical,phase: 5" \
  --body '## Context
Single source of truth for ML. Uses window functions (NOT self-joins) for historical aggregations. This ensures performance and explicit temporal boundaries.

## Tasks
- [ ] Create models/features/fct_order_features.sql
- [ ] Compute temporal, payment, product, geographic, freight features
- [ ] Historical seller features via window functions:
  ```sql
  AVG(delivery_days) OVER (
    PARTITION BY seller_id
    ORDER BY order_purchase_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS seller_avg_delivery_days_historical
  ```
- [ ] Include is_delayed target (null for non-delivered)
- [ ] Include order_purchase_timestamp for temporal splitting
- [ ] Materialize as table

## Definition of Done
- [ ] Model runs without errors
- [ ] No self-joins for historical features — window functions only
- [ ] is_delayed distribution ~7-10%

## References
- Feature design from Issue #18'

gh issue create --repo "${REPO}" \
  --title "feat: export features to Parquet and create anti-leakage test" \
  --milestone "${MS5}" \
  --label "type: feature,layer: features,priority: high,phase: 5" \
  --body '## Context
Parquet export decouples dbt from Python ML pipeline. Anti-leakage test is the safety net.

## Tasks
- [ ] Create scripts/export_features.py (COPY fct_order_features TO parquet)
- [ ] Add export-features target to Makefile
- [ ] Create tests/assert_no_leakage_in_features.sql
- [ ] Verify no feature derives from post-purchase timestamps
- [ ] Verify historical features use ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING

## Definition of Done
- [ ] `make export-features` produces data/ml/features.parquet
- [ ] Anti-leakage test passes
- [ ] ML scripts can read the Parquet file

## References
- [DuckDB COPY statement](https://duckdb.org/docs/sql/statements/copy)'

# --- PHASE 6 ISSUES ---
MS6=$(get_milestone "6")

gh issue create --repo "${REPO}" \
  --title "feat: implement data loading and temporal split" \
  --milestone "${MS6}" \
  --label "type: feature,layer: ml,priority: critical,phase: 6" \
  --body '## Context
Temporal splitting prevents leakage in time-ordered data. The model reads from Parquet (not DuckDB directly), respecting the analytics/ML boundary.

## Tasks
- [ ] Create ml/train.py reading from data/ml/features.parquet
- [ ] Filter to is_delayed IS NOT NULL
- [ ] Split by order_purchase_timestamp:
  - Train: < 2018-04-01
  - Validation: 2018-04-01 to 2018-06-30
  - Test: >= 2018-07-01
- [ ] Print split sizes and class balance

## Definition of Done
- [ ] Script runs from `make ml-train`
- [ ] Reads Parquet (not DuckDB)
- [ ] Split sizes reasonable, class balance consistent

## References
- docs/ml_design.md, data/ml/features.parquet'

gh issue create --repo "${REPO}" \
  --title "feat: train logistic regression baseline and evaluate" \
  --milestone "${MS6}" \
  --label "type: feature,layer: ml,priority: critical,phase: 6" \
  --body '## Context
Logistic regression baseline validates the feature pipeline and sets the performance floor.

## Tasks
- [ ] Build sklearn.Pipeline with ColumnTransformer (impute + encode + scale) and LogisticRegression
- [ ] Train on train set, evaluate on validation: ROC-AUC, precision, recall, F1, confusion matrix
- [ ] Save model to ml/models/model_v1_logreg.joblib
- [ ] Create ml/current_model.yaml with version, path, metrics
- [ ] Save training data snapshot to ml/data_snapshots/train_v1.parquet
- [ ] Generate ml/reports/evaluation_v1.md
- [ ] After validation: evaluate on test set and append results

## Definition of Done
- [ ] Model artifact saved and loadable
- [ ] current_model.yaml points to v1
- [ ] Training data snapshot exists for reproducibility
- [ ] Evaluation report complete with all metrics

## References
- [scikit-learn Pipeline](https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html)'

# --- PHASE 7 ISSUES ---
MS7=$(get_milestone "7")

gh issue create --repo "${REPO}" \
  --title "feat: build batch inference script with model registry" \
  --milestone "${MS7}" \
  --label "type: feature,layer: ml,priority: high,phase: 7" \
  --body '## Context
Inference reads current_model.yaml to determine the active model. This ensures retraining has practical effect — updating the YAML switches the production model.

## Tasks
- [ ] Create ml/predict.py
- [ ] Read current_model.yaml for model path and version
- [ ] Load model, load features from data/ml/features.parquet
- [ ] Generate predicted_probability and predicted_class (threshold 0.5)
- [ ] Add model_version and prediction_timestamp columns
- [ ] Save to ml/predictions/predictions_{version}_{date}.parquet
- [ ] Also write to DuckDB ml_predictions table

## Definition of Done
- [ ] Runs without errors
- [ ] Uses current_model.yaml (not hardcoded path)
- [ ] Output row count matches input
- [ ] Probabilities in [0, 1]

## References
- ml/current_model.yaml, data/ml/features.parquet'

gh issue create --repo "${REPO}" \
  --title "feat: create dbt source for predictions and bridge model" \
  --milestone "${MS7}" \
  --label "type: feature,layer: features,priority: medium,phase: 7" \
  --body '## Context
Predictions should be queryable alongside analytical models.

## Tasks
- [ ] Declare ml_predictions as dbt source
- [ ] Create models/features/fct_predictions.sql
- [ ] Add join example in docs/runbook.md

## Definition of Done
- [ ] `dbt build --select fct_predictions` passes
- [ ] Example query joining predictions to fact_orders works

## References
- ml/predict.py output schema'

# --- PHASE 8 ISSUES ---
MS8=$(get_milestone "8")

gh issue create --repo "${REPO}" \
  --title "feat: build model monitoring with business metrics" \
  --milestone "${MS8}" \
  --label "type: feature,layer: ml,priority: high,phase: 8" \
  --body '## Context
Monitoring must include both technical metrics and business impact. precision@k and cost analysis connect model performance to business decisions.

## Tasks
- [ ] Create ml/monitor.py
- [ ] Join predictions to actuals
- [ ] Compute per-window (monthly): ROC-AUC, precision, recall, F1
- [ ] Add precision@10% (top decile precision)
- [ ] Add cost analysis:
  - Define FP cost and FN cost in ml/config.yaml
  - FP = unnecessary intervention on order predicted delayed but not delayed
  - FN = missed delay, customer impact
  - Compute total estimated cost per window
- [ ] Define threshold: alert if ROC-AUC drops below baseline - 0.05
- [ ] Output to ml/reports/monitoring_{date}.json

## Definition of Done
- [ ] Technical metrics computed per time window
- [ ] Business metrics (precision@k, cost analysis) included
- [ ] Threshold logic works
- [ ] Output file exists

## References
- ml_predictions, fact_orders, ml/config.yaml'

gh issue create --repo "${REPO}" \
  --title "feat: implement drift detection with explicit baseline" \
  --milestone "${MS8}" \
  --label "type: feature,layer: ml,priority: medium,phase: 8" \
  --body '## Context
Drift detection needs a clear baseline (training data distribution), defined thresholds, and regular execution.

## Tasks
- [ ] Load baseline distributions from ml/data_snapshots/train_v1.parquet
- [ ] Compute PSI for numeric features between baseline and current features
- [ ] Compute frequency comparison for categorical features
- [ ] Apply thresholds:
  - PSI < 0.1 → OK
  - 0.1 <= PSI < 0.2 → warning
  - PSI >= 0.2 → alert (retraining trigger candidate)
- [ ] Periodicity: runs with every pipeline execution
- [ ] Output ml/reports/drift_{date}.md

## Definition of Done
- [ ] Baseline is explicitly the training data snapshot
- [ ] PSI computed for all numeric features
- [ ] Thresholds applied and results categorized
- [ ] Report is human-readable with OK/warning/alert labels

## References
- [PSI reference](https://www.listendata.com/2015/05/population-stability-index.html)
- ml/data_snapshots/train_v1.parquet'

# --- PHASE 9 ISSUES ---
MS9=$(get_milestone "9")

gh issue create --repo "${REPO}" \
  --title "feat: define retraining triggers and build retrain script" \
  --milestone "${MS9}" \
  --label "type: feature,layer: ml,priority: high,phase: 9" \
  --body '## Context
Clear triggers ensure retraining is systematic, not ad-hoc.

## Tasks
- [ ] Document triggers in docs/ml_design.md:
  - Performance trigger: ROC-AUC below baseline - 0.05 for 2+ consecutive windows
  - Drift trigger: 3+ features with PSI >= 0.2
  - Calendar trigger: every N months (safety net)
- [ ] Create ml/retrain.py with trigger evaluation
- [ ] On trigger: retrain with extended data window
- [ ] Save new model: model_v{N}_{algo}.joblib
- [ ] Save training data snapshot: ml/data_snapshots/train_v{N}.parquet
- [ ] Add logging for trigger decisions

## Definition of Done
- [ ] Evaluates all trigger types
- [ ] When triggered: new model + data snapshot saved
- [ ] When not triggered: clean exit with log

## References
- ml/monitor.py output, docs/ml_design.md'

gh issue create --repo "${REPO}" \
  --title "feat: implement model comparison and promotion" \
  --milestone "${MS9}" \
  --label "type: feature,layer: ml,priority: high,phase: 9" \
  --body '## Context
New models must prove improvement before replacing the active one. current_model.yaml is the promotion mechanism.

## Tasks
- [ ] Compare old vs new on same test set
- [ ] Generate comparison table: metric | old | new | delta
- [ ] Promotion rule: update current_model.yaml only if ROC-AUC improves
- [ ] Save report: ml/reports/comparison_{old}_vs_{new}.md

## Definition of Done
- [ ] Comparison runs and produces report
- [ ] Worse model is NOT promoted
- [ ] current_model.yaml reflects the winner

## References
- ml/models/, evaluation functions'

gh issue create --repo "${REPO}" \
  --title "feat: add ML checks to CI" \
  --milestone "${MS9}" \
  --label "type: feature,layer: infra,priority: high,phase: 9" \
  --body '## Context
ML breaks silently. CI must validate that models load, pipelines run, and metrics meet minimum thresholds.

## Tasks
- [ ] Add ML validation step to .github/workflows/ci.yml:
  - Check: model loads without error
  - Check: predict pipeline runs (dry-run mode)
  - Check: ROC-AUC above minimum threshold (e.g., 0.55)
- [ ] Ensure CI fails fast on any ML validation error

## Definition of Done
- [ ] CI fails if model cannot load
- [ ] CI fails if predict pipeline errors
- [ ] CI fails if AUC below threshold
- [ ] CI passes on happy path

## References
- Existing CI workflow'

gh issue create --repo "${REPO}" \
  --title "feat: full pipeline orchestration and final documentation" \
  --milestone "${MS9}" \
  --label "type: feature,layer: infra,priority: high,phase: 9" \
  --body '## Context
The entire system must run as a single command. Documentation must be portfolio-ready.

## Tasks
- [ ] Create/update scripts/run_pipeline.sh with full cycle: dbt build → export features → predict → monitor → retrain
- [ ] Update Makefile with pipeline target
- [ ] Write docs/runbook.md with:
  - Step-by-step execution guide
  - Benchmark results: typical dbt build time, predict.py time, monitor.py time
- [ ] Complete README.md with overview, quick-start, architecture, example queries
- [ ] Create CHANGELOG.md summarizing all phases

## Definition of Done
- [ ] `make pipeline` runs end to end
- [ ] README.md makes sense to a recruiter or senior engineer
- [ ] docs/runbook.md includes benchmark results
- [ ] CHANGELOG.md covers all phases

## References
- All prior phases'

echo "  ✅ All 30 issues created"

# ============================================================================
# 7. SUMMARY
# ============================================================================
echo ""
echo "============================================"
echo "  ✅ SETUP COMPLETE"
echo "============================================"
echo ""
echo "  Repository: https://github.com/${REPO}"
echo "  Milestones: 10 created"
echo "  Labels:     30+ created"
echo "  Issues:     30 created"
echo "  Templates:  2 issue + 1 PR"
echo "  CI:         placeholder workflow"
echo "  Protection: main branch protected"
echo ""
echo "  Next step: git checkout -b phase/0-setup"
echo "============================================"
