# ==============================================================================
# Makefile — Apple & Tesla Market Intelligence Pipeline
#
# Prerequisites (host machine only):
#   - Docker Desktop (running)
#   - Terraform >= 1.5
#   - gcloud CLI (for GCP authentication)
#
# Everything else — Python, dbt, flake8, black, dashboard deps — runs inside
# Docker containers. No pip install required on the host.
# ==============================================================================

.PHONY: help setup env-check \
        tf-init tf-plan tf-apply tf-destroy \
        credentials-dir \
        airflow-fernet airflow-init airflow-up airflow-down airflow-logs \
        dbt-deps dbt-run dbt-test dbt-docs \
        pipeline-trigger pipeline-status \
        dashboard lint test clean

# ── Defaults ──────────────────────────────────────────────────────────────────
SHELL        := /bin/bash
PROJECT_DIR  := $(shell pwd)
ENV_FILE     := $(PROJECT_DIR)/.env
AIRFLOW_DIR  := $(PROJECT_DIR)/airflow
TF_DIR       := $(PROJECT_DIR)/terraform
DBT_DIR      := $(PROJECT_DIR)/dbt
COMPOSE      := docker compose --env-file $(ENV_FILE) -f $(AIRFLOW_DIR)/docker-compose.yml

# Load .env if it exists (so make targets can use env vars)
ifneq (,$(wildcard $(ENV_FILE)))
    include $(ENV_FILE)
    export
endif

# ── Help ──────────────────────────────────────────────────────────────────────
help: ## Show this help message
	@echo ""
	@echo "  Apple & Tesla Market Intelligence Pipeline"
	@echo "  ══════════════════════════════════════════"
	@echo "  Prerequisites: Docker (running), Terraform >= 1.5, gcloud CLI"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	    | sort \
	    | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ── One-time setup ─────────────────────────────────────────────────────────────
setup: env-check credentials-dir tf-init airflow-fernet ## Full first-time setup (run once)
	@echo ""
	@echo "✅ Setup complete. Next steps:"
	@echo "   1. Edit .env with your real values"
	@echo "   2. make tf-apply      — create GCP infrastructure"
	@echo "   3. make airflow-init  — initialise the Airflow database"
	@echo "   4. make airflow-up    — start Airflow"
	@echo "   5. make pipeline-trigger — run the pipeline"
	@echo ""

env-check: ## Check that .env exists; create from example if not
	@if [ ! -f $(ENV_FILE) ]; then \
	    cp $(PROJECT_DIR)/.env.example $(ENV_FILE); \
	    echo "⚠️  Created .env from .env.example — please fill in your values."; \
	else \
	    echo "✅ .env found."; \
	fi

credentials-dir: ## Create the credentials/ directory (gitignored)
	@mkdir -p $(AIRFLOW_DIR)/credentials
	@echo "✅ credentials/ directory ready."

# ── Terraform ─────────────────────────────────────────────────────────────────
tf-init: ## Initialise Terraform (download providers)
	@echo "→ Running terraform init..."
	@cd $(TF_DIR) && terraform init

tf-plan: ## Preview infrastructure changes
	@cd $(TF_DIR) && env -u GOOGLE_APPLICATION_CREDENTIALS terraform plan \
	    -var="project_id=$(GCP_PROJECT_ID)"

tf-apply: ## Create GCP infrastructure (GCS, BigQuery, IAM)
	@echo "→ Applying Terraform — this will create GCP resources..."
	@cd $(TF_DIR) && env -u GOOGLE_APPLICATION_CREDENTIALS terraform apply \
	    -var="project_id=$(GCP_PROJECT_ID)" \
	    -auto-approve
	@echo "✅ Infrastructure provisioned."
	@echo "   GCS bucket  : $$(cd $(TF_DIR) && terraform output -raw gcs_bucket_name)"
	@echo "   BQ dataset  : $$(cd $(TF_DIR) && terraform output -raw bigquery_dataset_id)"
	@echo "   SA email    : $$(cd $(TF_DIR) && terraform output -raw service_account_email)"

tf-destroy: ## ⚠️  Destroy ALL GCP infrastructure (irreversible)
	@read -p "Are you sure you want to destroy all infrastructure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
	    cd $(TF_DIR) && env -u GOOGLE_APPLICATION_CREDENTIALS terraform destroy \
	        -var="project_id=$(GCP_PROJECT_ID)" \
	        -auto-approve; \
	else \
	    echo "Aborted."; \
	fi

# ── Airflow ───────────────────────────────────────────────────────────────────
airflow-fernet: ## Generate a Fernet key and add it to .env (uses Docker — no host Python needed)
	@echo "→ Generating Fernet key via Docker..."
	@FERNET=$$(docker run --rm python:3.11-slim python -c \
	    "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
	if grep -q "^AIRFLOW_FERNET_KEY=" $(ENV_FILE) 2>/dev/null; then \
	    sed -i.bak "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$FERNET|" $(ENV_FILE); \
	else \
	    echo "AIRFLOW_FERNET_KEY=$$FERNET" >> $(ENV_FILE); \
	fi; \
	echo "✅ Fernet key written to .env."

airflow-init: ## Initialise Airflow DB and create admin user (run once)
	@echo "→ Initialising Airflow..."
	@AIRFLOW_UID=$$(id -u) $(COMPOSE) up airflow-init --exit-code-from airflow-init
	@echo "✅ Airflow initialised. Login: admin / admin"

airflow-up: ## Start Airflow services (webserver + scheduler)
	@echo "→ Starting Airflow..."
	@AIRFLOW_UID=$$(id -u) $(COMPOSE) up webserver scheduler -d
	@echo "✅ Airflow UI: http://localhost:8080  (admin / admin)"

airflow-down: ## Stop all Airflow services
	@$(COMPOSE) down

airflow-logs: ## Tail Airflow scheduler logs
	@$(COMPOSE) logs -f scheduler

# ── dbt — runs inside the scheduler container (no local dbt install needed) ───
#
# Requirements: Airflow must be running (`make airflow-up` first).
# dbt is installed in the scheduler container via airflow/requirements.txt.
# The dbt/ directory is mounted at /opt/airflow/dbt inside the container.
# ─────────────────────────────────────────────────────────────────────────────
dbt-deps: ## Install dbt packages inside the running scheduler container
	@$(COMPOSE) exec scheduler \
	    dbt deps --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

dbt-run: ## Run dbt models inside the running scheduler container
	@$(COMPOSE) exec scheduler \
	    dbt run \
	    --profiles-dir /opt/airflow/dbt \
	    --project-dir /opt/airflow/dbt

dbt-test: ## Run dbt data quality tests inside the running scheduler container
	@$(COMPOSE) exec scheduler \
	    dbt test \
	    --profiles-dir /opt/airflow/dbt \
	    --project-dir /opt/airflow/dbt

dbt-docs: ## Generate dbt docs (in container) and serve at http://localhost:8081 (on host)
	@echo "→ Generating dbt docs inside scheduler container..."
	@$(COMPOSE) exec scheduler \
	    dbt docs generate \
	    --profiles-dir /opt/airflow/dbt \
	    --project-dir /opt/airflow/dbt
	@echo "✅ Docs generated. Serving at http://localhost:8081 (Ctrl+C to stop)"
	@cd $(DBT_DIR)/target && python3 -m http.server 8081

# ── Pipeline ──────────────────────────────────────────────────────────────────
pipeline-trigger: ## Trigger the daily_market_pipeline DAG for today
	@$(COMPOSE) exec webserver \
	    airflow dags trigger daily_market_pipeline \
	    --exec-date "$$(date -u +%Y-%m-%dT%H:%M:%S)"
	@echo "✅ Pipeline triggered. Monitor at http://localhost:8080"

pipeline-status: ## Show the last 5 DAG runs
	@$(COMPOSE) exec webserver \
	    airflow dags list-runs -d daily_market_pipeline --limit 5

# ── Dashboard — runs in a dedicated Docker container ──────────────────────────
dashboard: ## Build and run the dashboard container → writes dashboard/dashboard.html
	@echo "→ Building dashboard image..."
	@docker build -q -t market-pipeline-dashboard $(PROJECT_DIR)/dashboard
	@echo "→ Generating dashboard (queries BigQuery)..."
	@docker run --rm \
	    --env-file $(ENV_FILE) \
	    -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service_account.json \
	    -v $(AIRFLOW_DIR)/credentials:/credentials:ro \
	    -v $(PROJECT_DIR)/dashboard:/app \
	    -v $(PROJECT_DIR)/screenshots:/screenshots \
	    market-pipeline-dashboard
	@echo "✅ Dashboard ready."
	@open $(PROJECT_DIR)/dashboard/dashboard.html

# ── Quality ───────────────────────────────────────────────────────────────────
lint: ## Run flake8 and black inside the running scheduler container
	@echo "→ Running flake8..."
	@$(COMPOSE) exec scheduler \
	    flake8 /opt/airflow/src/ /opt/airflow/dags/ --max-line-length=120
	@echo "→ Running black check..."
	@$(COMPOSE) exec scheduler \
	    black --check /opt/airflow/src/ /opt/airflow/dags/

test: ## Run unit tests (uses local Python — only needs pytest, pandas, requests)
	@pytest tests/ -v

# ── Housekeeping ──────────────────────────────────────────────────────────────
clean: ## Remove local temp files and dbt artifacts
	@rm -rf /tmp/market_data
	@rm -rf $(DBT_DIR)/target
	@rm -rf $(DBT_DIR)/dbt_packages
	@echo "✅ Cleaned local artifacts."
