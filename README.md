# tutorful_task

This repository contains a small dbt-based analytics workflow on BigQuery.

It is organised into three dbt layers:

- `dbt/tutorful_raw`: ingests CSV data into BigQuery using `dbt seed`
- `dbt/tutorful_staging`: cleans and standardises the seeded raw tables
- `dbt/tutorful_curated`: builds business-facing churn and reactivation models from the staged data

The project context is student lesson activity.
The main business definitions used in the curated layer are:

- `Churn`: a student that has not completed a lesson for 30 days
- `Reactivation`: a student with a completed lesson who had not had a lesson for 30 days before that completed lesson

A student can churn, reactivate, and churn again, so the lifecycle event models allow multiple churn/reactivation events per student over time.

## Repository Structure

```text
.
├── dbt/
│   ├── tutorful_raw/
│   ├── tutorful_staging/
│   └── tutorful_curated/
├── sim_data/
└── README.md
```

### `dbt/tutorful_raw`

This project is mainly used to ingest CSV files into BigQuery with `dbt seed`.

Current seed files:

- `bookings.csv`
- `lessons.csv`
- `relationship.csv`
- `subjects.csv`

These CSV files are stored in [`dbt/tutorful_raw/seeds/`](/Users/shahanahmed/Desktop/tutorful_task/dbt/tutorful_raw/seeds).

The raw project is useful for loading test or sample data into a BigQuery dataset without needing a separate ingestion tool.

### `dbt/tutorful_staging`

This project reads from the raw BigQuery tables and builds cleaned staging tables.

Current staging models:

- `bookings`
- `lessons`
- `relationship`
- `subjects`

Notable staging logic:

- parses booking timestamps from strings like `01/01/2025 05:00`
- casts seeded raw string columns into the appropriate BigQuery types
- uses incremental materialisations where appropriate
- defines unique keys to avoid appending duplicate rows across runs

### `dbt/tutorful_curated`

This project reads from the staging dataset and produces churn/reactivation outputs.

Current curated models:

- `int_completed_lessons`
- `int_student_lesson_gaps`
- `fct_student_lifecycle_events`
- `mart_churn_monthly`
- `churn_feb2026`
- `reactivation_feb2026`

What each model does:

- `int_completed_lessons`: joins bookings, lessons, and relationship, then filters to completed lessons
- `int_student_lesson_gaps`: calculates the gap since the previous completed lesson for each student
- `fct_student_lifecycle_events`: creates `churn` and `reactivation` lifecycle events
- `mart_churn_monthly`: monthly summary of lifecycle events
- `churn_feb2026`: churn events occurring in February 2026
- `reactivation_feb2026`: reactivation events occurring in February 2026

## Data Flow

The intended flow is:

1. CSV files are loaded into BigQuery using `dbt seed` in `tutorful_raw`
2. `tutorful_staging` builds cleaned tables in a staging dataset
3. `tutorful_curated` reads those staging tables and builds analytics tables in a curated dataset

Conceptually:

```text
CSV files -> raw seed dataset -> staging dataset -> curated dataset
```

## BigQuery / dbt Setup

This repo assumes:

- you have Python and a local virtual environment available
- `dbt-bigquery` is installed in the virtual environment
- you can authenticate to GCP locally
- the target BigQuery datasets already exist, or are created separately

Terraform is not required to run dbt, but may be used separately to manage datasets and tables.

### Authentication

Typical local auth flow:

```bash
gcloud auth application-default login
gcloud config set project tutorful-493614
gcloud auth application-default set-quota-project tutorful-493614
```

That last command helps avoid quota-project warnings when dbt connects through ADC credentials.

## Profiles and Datasets

The repo currently uses separate dbt profiles for each layer.

### Raw profile

The raw project uses the `tutorful_raw` profile and is intended to seed CSV data into a BigQuery dataset such as `tutorful_seed`.

### Staging profile

The staging project uses the `tutorful_staging` profile and reads from the raw dataset.

### Curated profile

The curated project uses the `tutorful_curated` profile and reads from the staging dataset.

## Environment Variables

The staging and curated source configuration uses environment variables for project and dataset names.

Common variables used in this repo:

- `BQ_PROJECT_ID`
- `BQ_DATASET_ID`

Current shell scripts set these as follows:

### Staging setup

[`dbt/tutorful_staging/setup.sh`](/Users/shahanahmed/Desktop/tutorful_task/dbt/tutorful_staging/setup.sh)

```bash
export BQ_PROJECT_ID="tutorful-493614"
export BQ_DATASET_ID="tutorful_raw"
```

This means the staging project reads from the raw dataset configured in the script.

### Curated setup

[`dbt/tutorful_curated/setup.sh`](/Users/shahanahmed/Desktop/tutorful_task/dbt/tutorful_curated/setup.sh)

```bash
export BQ_PROJECT_ID="tutorful-493614"
export BQ_DATASET_ID="tutorful_staging"
```

This means the curated project reads from the staging dataset.

## How To Run

### 1. Activate the virtual environment

From the repository root:

```bash
source .venv/bin/activate
```

### 2. Run the full pipeline

The simplest way to run everything end to end is:

```bash
bash dbt/run_pipeline.sh
```

This script runs the full pipeline in order:

1. seeds the raw CSV data in `tutorful_raw`
2. runs the staging models in `tutorful_staging`
3. runs the curated models in `tutorful_curated`
4. prints the February 2026 churn and reactivation summary

At the end of the pipeline run, the script prints:

- total churn events in February 2026
- total reactivation events in February 2026
- daily churn and reactivation volumes for February 2026

If you need to rebuild all incremental models and seeds from scratch, use:

```bash
bash dbt/run_pipeline.sh --full-refresh
```

### 3. Run each layer manually

If you want to run the layers one by one instead, use the following steps.

#### Seed the raw CSV data

```bash
cd dbt/tutorful_raw
dbt debug
dbt seed --show
```

This loads the CSV files in `seeds/` into the dataset configured by the `tutorful_raw` profile.

#### Run the staging project

```bash
cd ../tutorful_staging
source setup.sh
bash project_run.sh
```

If the schema has changed since the last run, use:

```bash
dbt run --full-refresh
```

#### Run the curated project

```bash
cd ../tutorful_curated
source setup.sh
bash project_run.sh
```

If the schema has changed since the last run, use:

```bash
dbt run --full-refresh
```

This builds the churn/reactivation models on top of staging.

## Useful dbt Commands

Run a single model:

```bash
dbt run -s bookings
dbt run -s fct_student_lifecycle_events
dbt run -s churn_feb2026
dbt run -s reactivation_feb2026
```

Parse the project without running models:

```bash
dbt parse
```

Run tests:

```bash
dbt test
```

Reload seeds cleanly:

```bash
dbt seed --full-refresh --show
```

## Key Outputs

Important curated tables include:

- `tutorful_curated.fct_student_lifecycle_events`
- `tutorful_curated.mart_churn_monthly`
- `tutorful_curated.churn_feb2026`
- `tutorful_curated.reactivation_feb2026`

### Which table answers which question?

For churn in February 2026:

- `tutorful_curated.churn_feb2026`

For reactivation in February 2026:

- `tutorful_curated.reactivation_feb2026`

For the reusable all-time event history:

- `tutorful_curated.fct_student_lifecycle_events`

## Assumptions

Current curated logic assumes:

- lesson activity is based on completed bookings only
- the activity timestamp is `finish_at`
- churn/reactivation is measured at the `student_id` level
- students can have multiple churn/reactivation events over time

## Notes

- Some models are intentionally materialised as incremental tables so the outputs are persisted in BigQuery
- The `tutorful_raw` project is now primarily an ingestion layer for CSV seeds
- If seed columns are inferred as strings in BigQuery, the staging models handle the type casting
- If dataset or project names change, update the relevant profile or shell scripts before running dbt
- BigQuery dataset location must match the profile location used by dbt

## Next Improvements

Potential follow-up improvements for this repo:

1. add more dbt tests for lifecycle event quality
2. parameterise month-specific models instead of hard-coding February 2026
3. add Terraform to manage BigQuery datasets and tables consistently
4. document the expected dataset locations and profile settings more explicitly
