#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FULL_REFRESH="${1:-}"

if [[ "${FULL_REFRESH}" != "" && "${FULL_REFRESH}" != "--full-refresh" ]]; then
  echo "Usage: bash dbt/run_pipeline.sh [--full-refresh]"
  exit 1
fi

if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # Use the repo-local virtual environment when available.
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "dbt was not found on PATH. Activate the virtual environment first."
  exit 1
fi

run_raw() {
  echo ""
  echo "==> Seeding tutorful_raw"
  (
    cd "${SCRIPT_DIR}/tutorful_raw"
    dbt deps
    dbt seed ${FULL_REFRESH:+--full-refresh} --show
  )
}

run_staging() {
  echo ""
  echo "==> Running tutorful_staging"
  (
    cd "${SCRIPT_DIR}/tutorful_staging"
    # shellcheck disable=SC1091
    source setup.sh
    dbt deps
    dbt run ${FULL_REFRESH:+--full-refresh}
  )
}

run_curated() {
  echo ""
  echo "==> Running tutorful_curated"
  (
    cd "${SCRIPT_DIR}/tutorful_curated"
    # shellcheck disable=SC1091
    source setup.sh
    dbt deps
    dbt run ${FULL_REFRESH:+--full-refresh}
  )
}

report_february_2026() {
  echo ""
  echo "==> February 2026 churn and reactivation summary"
  (
    cd "${SCRIPT_DIR}/tutorful_curated"
    # shellcheck disable=SC1091
    source setup.sh

    echo ""
    echo "Overall volumes"
    dbt show --inline "
      select 'churn_feb2026' as metric, count(*) as total_events
      from {{ ref('churn_feb2026') }}
      union all
      select 'reactivation_feb2026' as metric, count(*) as total_events
      from {{ ref('reactivation_feb2026') }}
    "

    echo ""
    echo "Daily volumes"
    dbt show --inline "
      select
        date(event_at) as event_date,
        event_type,
        count(*) as daily_events
      from {{ ref('fct_student_lifecycle_events') }}
      where date(event_at) >= date '2026-02-01'
        and date(event_at) < date '2026-03-01'
      group by 1, 2
      order by 1, 2
    "
  )
}

echo "Running the dbt pipeline end to end..."

run_raw
run_staging
run_curated
report_february_2026

echo ""
echo "Pipeline complete."
