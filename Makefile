# ============================================================
# NexusFlow DataOps Portfolio — Master Makefile
# Usage: make deploy ENV=dev | make destroy ENV=dev
# ============================================================

ENV          ?= dev
AWS_REGION   ?= us-east-1
AWS_ACCOUNT  := $(shell aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY := $(AWS_ACCOUNT).dkr.ecr.$(AWS_REGION).amazonaws.com
PROJECT      := nexusflow
TF_DIR       := terraform/environments/$(ENV)

.PHONY: all deploy destroy build push validate lint test docs help

## ── COLORS ────────────────────────────────────────────────
CYAN  := \033[0;36m
GREEN := \033[0;32m
RED   := \033[0;31m
NC    := \033[0m

help: ## Show this help
	@echo "$(CYAN)NexusFlow DataOps Portfolio$(NC)"
	@echo "Usage: make <target> ENV=dev|prod"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

## ── FULL DEPLOY ───────────────────────────────────────────
deploy: check-prereqs infra-init infra-apply ecr-login build push k8s-deploy validate ## 🚀 Full stack deploy (push-button)
	@echo "$(GREEN)✅ NexusFlow deployed to $(ENV)$(NC)"
	@echo "$(GREEN)   Airflow UI:   http://$(shell kubectl get svc airflow-webserver -n airflow -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')$(NC)"
	@echo "$(GREEN)   Grafana:      http://$(shell kubectl get svc grafana -n monitoring -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')$(NC)"
	@echo "$(GREEN)   Analytics API:http://$(shell kubectl get svc nexusflow-api -n nexusflow -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')$(NC)"

## ── PREREQUISITES ─────────────────────────────────────────
check-prereqs: ## Verify all required tools are installed
	@echo "$(CYAN)Checking prerequisites...$(NC)"
	@command -v terraform >/dev/null 2>&1 || { echo "$(RED)terraform not found$(NC)"; exit 1; }
	@command -v kubectl   >/dev/null 2>&1 || { echo "$(RED)kubectl not found$(NC)"; exit 1; }
	@command -v helm      >/dev/null 2>&1 || { echo "$(RED)helm not found$(NC)"; exit 1; }
	@command -v aws       >/dev/null 2>&1 || { echo "$(RED)aws-cli not found$(NC)"; exit 1; }
	@command -v docker    >/dev/null 2>&1 || { echo "$(RED)docker not found$(NC)"; exit 1; }
	@echo "$(GREEN)All prerequisites satisfied$(NC)"

## ── TERRAFORM ─────────────────────────────────────────────
infra-init: ## Terraform init
	@echo "$(CYAN)Initializing Terraform...$(NC)"
	cd $(TF_DIR) && terraform init -upgrade

infra-plan: ## Terraform plan
	cd $(TF_DIR) && terraform plan -var-file=terraform.tfvars -out=tfplan

infra-apply: ## Terraform apply (auto-approve)
	@echo "$(CYAN)Provisioning AWS infrastructure...$(NC)"
	cd $(TF_DIR) && terraform apply -var-file=terraform.tfvars -auto-approve
	cd $(TF_DIR) && terraform output -json > /tmp/tf_outputs.json
	@echo "$(GREEN)Infrastructure provisioned$(NC)"

infra-destroy: ## Terraform destroy
	@echo "$(RED)Destroying infrastructure in $(ENV)...$(NC)"
	cd $(TF_DIR) && terraform destroy -var-file=terraform.tfvars -auto-approve

## ── DOCKER / ECR ──────────────────────────────────────────
ecr-login: ## Login to ECR
	aws ecr get-login-password --region $(AWS_REGION) | \
		docker login --username AWS --password-stdin $(ECR_REGISTRY)

build: ## Build all Docker images
	@echo "$(CYAN)Building Docker images...$(NC)"
	docker build -t $(PROJECT)-datagen:latest        src/datagen/
	docker build -t $(PROJECT)-ingestion:latest      src/ingestion/
	docker build -t $(PROJECT)-processing:latest     src/processing/
	docker build -t $(PROJECT)-dbt:latest            dbt_project/
	docker build -t $(PROJECT)-serving:latest        src/serving/
	docker build -t $(PROJECT)-dashboard:latest      src/dashboard/
	@echo "$(GREEN)All images built$(NC)"

push: ## Tag and push images to ECR
	@echo "$(CYAN)Pushing images to ECR...$(NC)"
	@for svc in datagen ingestion processing dbt serving dashboard; do \
		docker tag $(PROJECT)-$$svc:latest $(ECR_REGISTRY)/$(PROJECT)-$$svc:latest; \
		docker push $(ECR_REGISTRY)/$(PROJECT)-$$svc:latest; \
		echo "  ✓ Pushed $$svc"; \
	done

## ── KUBERNETES ────────────────────────────────────────────
kubeconfig: ## Update kubeconfig for EKS
	aws eks update-kubeconfig --region $(AWS_REGION) --name $(PROJECT)-$(ENV)-cluster

k8s-deploy: kubeconfig ## Deploy all Kubernetes services
	@echo "$(CYAN)Deploying to EKS...$(NC)"
	kubectl apply -f kubernetes/monitoring/namespace.yml
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo add grafana https://grafana.github.io/helm-charts
	helm repo update
	# Monitoring stack
	helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
		-n monitoring --create-namespace -f kubernetes/monitoring/values.yml
	# Airflow
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow \
		-n airflow --create-namespace -f kubernetes/airflow/values.yml
	# Application services
	kubectl apply -f kubernetes/kafka/
	kubectl apply -f kubernetes/datagen/
	kubectl apply -f kubernetes/dbt/
	kubectl apply -f kubernetes/dashboard/
	@echo "$(GREEN)All services deployed$(NC)"

k8s-status: ## Show all pod statuses
	kubectl get pods -A

## ── DBT ───────────────────────────────────────────────────
dbt-run: ## Run dbt models
	cd dbt_project && dbt run --profiles-dir . --target $(ENV)

dbt-test: ## Run dbt tests
	cd dbt_project && dbt test --profiles-dir . --target $(ENV)

dbt-docs: ## Generate dbt docs
	cd dbt_project && dbt docs generate && dbt docs serve

## ── CODE QUALITY ──────────────────────────────────────────
lint: ## Lint Python (PEP8), SQL (sqlfluff), Terraform
	@echo "$(CYAN)Linting...$(NC)"
	flake8 src/ --max-line-length=100
	black --check src/
	isort --check src/
	sqlfluff lint dbt_project/models/ --dialect redshift
	cd terraform && terraform fmt -check -recursive

format: ## Auto-format all code
	black src/
	isort src/
	sqlfluff fix dbt_project/models/ --dialect redshift
	cd terraform && terraform fmt -recursive

test: ## Run all tests
	@echo "$(CYAN)Running tests...$(NC)"
	pytest src/ -v --cov=src --cov-report=html
	cd dbt_project && dbt test

## ── VALIDATION ────────────────────────────────────────────
validate: ## Run end-to-end validation
	@echo "$(CYAN)Running E2E validation...$(NC)"
	bash scripts/validate/e2e_test.sh $(ENV)

## ── FULL TEARDOWN ─────────────────────────────────────────
destroy: ## 🔥 Destroy everything (EKS → ECR → Infra)
	@echo "$(RED)WARNING: This will destroy all resources in $(ENV)$(NC)"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	bash scripts/teardown/destroy.sh $(ENV) $(AWS_REGION)
	$(MAKE) infra-destroy

## ── UTILITIES ─────────────────────────────────────────────
logs-airflow: ## Stream Airflow scheduler logs
	kubectl logs -n airflow -l component=scheduler -f

logs-datagen: ## Stream data generator logs
	kubectl logs -n nexusflow -l app=datagen -f

port-forward-airflow: ## Port-forward Airflow UI to localhost:8080
	kubectl port-forward -n airflow svc/airflow-webserver 8080:8080

port-forward-grafana: ## Port-forward Grafana to localhost:3000
	kubectl port-forward -n monitoring svc/grafana 3000:80
