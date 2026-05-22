# ============================================================
# NexusFlow — Dev Environment Orchestrator
#
# Terraform Cloud: NexusFlow_DataOps_Pipeline / ecom-infra-Dev
# ============================================================

terraform {
  required_version = ">= 1.8.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.28"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # ── TERRAFORM CLOUD BACKEND ─────────────────────────────
  # State is stored in Terraform Cloud — no S3 bucket needed.
  
  cloud {
    organization = "NexusFlow_DataOps_Pipeline" # TERRAFORM ORG NAME

    workspaces {
      name = "ecom-infra-Dev" # WORKSPACE NAME within TERRAFORM ORG NAME
    }
  }
}

# ── AWS PROVIDER ──────────────────────────────────────────
provider "aws" {
  region = var.aws_region # set in terraform.tfvars — default ca-central-1

  default_tags {
    tags = {
      Project     = "nexusflow"
      Environment = var.environment   # dev
      ManagedBy   = "terraform"
      Owner       = var.team_name     # smit-data-engineering-portfolio
    }
  }
}

# ── KUBERNETES PROVIDER ───────────────────────────────────
# Connects kubectl to EKS after cluster is created.
# Uses dynamic values from the EKS module output.
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

# ── HELM PROVIDER ─────────────────────────────────────────
# Used to deploy Airflow, Prometheus, Grafana into EKS.
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

# ── DATA SOURCES ──────────────────────────────────────────
# Fetches available AZs in chosen region (ca-central-1) automatically.
data "aws_availability_zones" "available" {
  state = "available"
}

# Fetches AWS account ID automatically.
data "aws_caller_identity" "current" {}

# ── LOCALS ────────────────────────────────────────────────
locals {
  name   = "${var.project_name}-${var.environment}" # nexusflow-dev
  azs    = slice(data.aws_availability_zones.available.names, 0, 3)
  region = var.aws_region

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ══════════════════════════════════════════════════════════
# MODULE 1 — VPC
# Creates: VPC, public/private subnets, NAT gateway, IGW
# All other modules depend on this running first.
# ══════════════════════════════════════════════════════════
module "vpc" {
  source = "../../modules/vpc"

  name            = local.name
  cidr            = var.vpc_cidr           # 10.0.0.0/16 — change if conflicts with existing VPC
  azs             = local.azs              # auto-selected from your region
  private_subnets = var.private_subnets    # ["10.0.1.0/24","10.0.2.0/24","10.0.3.0/24"]
  public_subnets  = var.public_subnets     # ["10.0.101.0/24","10.0.102.0/24","10.0.103.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true  # 1 NAT gateway for dev could save ~$32/mo vs 3 gateways ($,single/multi AZ)
                             # CHANGE single_nat_gateway = false for prod (high availability)

  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 2 — EKS
# Creates: Kubernetes cluster, node groups, IRSA, add-ons
# Depends on: VPC (needs subnet IDs)
# ══════════════════════════════════════════════════════════
module "eks" {
  source = "../../modules/eks"

  cluster_name    = "${local.name}-cluster"      # nexusflow-dev-cluster
  cluster_version = var.eks_cluster_version      # "1.29" — update if newer version available
  vpc_id          = module.vpc.vpc_id            # auto from VPC module
  subnet_ids      = module.vpc.private_subnets   # EKS nodes in private subnets only

  node_groups = {
    # ── GENERAL WORKLOADS (Airflow, API, datagen) ────────
    general = {
      instance_types = var.eks_node_instance_types # ["m5.xlarge","m5.2xlarge"]
                                                   # ⚠️ scale down to ["t3.medium"] to cut the cost
                                                   # during initial testing only
      min_size     = 2  # Keeping min 2 to maintain availability
      max_size     = 6  # ⚠️ increase for prod. 
      desired_size = 2  # ⚠️ increase for prod.
    }

    # ── SPARK WORKLOADS (EMR on EKS, isolated) ───────────
    spark = {
      instance_types = ["r6i.xlarge", "r6i.2xlarge"] # memory-optimized for Spark , SPOT instance
                                                     # ⚠️ choose ["r5.xlarge"] if r6i unavailable
      min_size     = 0  # if no Spark jobs running — saves cost
      max_size     = 10 # ⚠️ change based on number of prallel Spark job processing
      desired_size = 0  # starts at 0 — auto-scales when job submitted
      labels = {
        workload = "spark"
      }
      taints = [{
        key    = "workload"
        value  = "spark"
        effect = "NO_SCHEDULE" # only Spark pods schedule on these nodes
      }]
    }
  }

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 3 — ECR
# Creates: Container registries for all 6 services
# Depends on: nothing (standalone)
# ══════════════════════════════════════════════════════════
module "ecr" {
  source = "../../modules/ecr"

  # ⚠️ configure change here if any service is renamed.
  repositories = [
    "nexusflow-datagen",
    "nexusflow-ingestion",
    "nexusflow-processing",
    "nexusflow-dbt",
    "nexusflow-serving",
    "nexusflow-dashboard",
  ]

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 4 — S3
# Creates: bronze, silver, gold, artifacts, logs, athena buckets
# Depends on: nothing (standalone)
# ══════════════════════════════════════════════════════════
module "s3" {
  source = "../../modules/s3"

  project_name = var.project_name  # nexusflow
  environment  = var.environment   # dev

  # ⚠️ S3 bucket names must be GLOBALLY unique across all AWS accounts.
  # monitor "BucketAlreadyExists" error, add unique suffix 'name'to resolve.
  buckets = {
    lakehouse = {
      name              = "nexusflow-dev-lakehouse"     
      versioning        = true
      lifecycle_enabled = true
      transition_days   = 90  # move to STANDARD_IA after 90 days
    }
    artifacts = {
      name              = "nexusflow-dev-artifacts"     
      versioning        = true
      lifecycle_enabled = false
    }
    logs = {
      name              = "nexusflow-dev-logs"           
      versioning        = false
      lifecycle_enabled = true
      transition_days   = 30  # logs move to cheaper storage faster
    }
    athena_results = {
      name              = "nexusflow-dev-athena-results" 
      versioning        = false
      lifecycle_enabled = false
    }
  }

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 5 — MSK (Kafka)
# Creates: Managed Kafka cluster + topics
# Depends on: VPC (needs subnet IDs + security group)
# ══════════════════════════════════════════════════════════
module "msk" {
  source = "../../modules/msk"

  cluster_name  = "${local.name}-kafka"           # nexusflow-dev-kafka
  kafka_version = "3.6.0"                         # update for newer stable version.
  instance_type = "kafka.t3.small"                # budget friendly option for dev
                                                  # ⚠️ scale up to "kafka.m5.large" for prod
  broker_count  = 2                               # minimum for dev
                                                  # ⚠️ scale to 3 (common min) or 5 or higher for prod 
  vpc_id        = module.vpc.vpc_id
  subnet_ids    = module.vpc.private_subnets
  storage_gb    = 100                             # 100GB per broker for dev
                                                  # ⚠️ increase to 1000 or more for prod
  logs_bucket   = module.s3.bucket_names["logs"]  # auto from S3 module

  # Kafka topics — partitions drive parallelism
  # Rule: partitions >= number of consumers you expect
  topics = [
    { name = "orders",           partitions = 6,  replication = 2 },
    { name = "clickstream",      partitions = 12, replication = 2 }, # highest volume
    { name = "inventory-events", partitions = 4,  replication = 2 },
    { name = "product-reviews",  partitions = 4,  replication = 2 },
    { name = "user-sessions",    partitions = 8,  replication = 2 },
    { name = "dlq-orders",       partitions = 2,  replication = 2 }, # dead letter queue
  ]

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 6 — GLUE
# Creates: Data catalog databases + crawlers for schema discovery
# Depends on: S3 (needs lakehouse bucket name)
# ══════════════════════════════════════════════════════════
module "glue" {
  source = "../../modules/glue"

  project_name     = var.project_name
  environment      = var.environment
  lakehouse_bucket = module.s3.bucket_names["lakehouse"] # auto from S3 module

  # Creates one Glue database per medallion layer
  # ⚠️ update names only if medallion layers renamed.
  database_names = ["bronze", "silver", "gold"]

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 7 — EMR SERVERLESS
# Creates: Spark processing application (pay per job, no idle cost)
# Depends on: VPC, S3
# ══════════════════════════════════════════════════════════
module "emr" {
  source = "../../modules/emr"

  project_name     = local.name
  environment      = var.environment
  subnet_id        = module.vpc.private_subnets[0]         # single subnet for dev
  logs_bucket      = module.s3.bucket_names["logs"]        # auto from S3 module
  artifacts_bucket = module.s3.bucket_names["artifacts"]   # auto from S3 module

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 8 — REDSHIFT SERVERLESS
# Creates: Data warehouse for gold layer serving
# Depends on: VPC
# ══════════════════════════════════════════════════════════
module "redshift" {
  source = "../../modules/redshift"

  namespace_name = "${local.name}-ns"  # nexusflow-dev-ns
  workgroup_name = "${local.name}-wg"  # nexusflow-dev-wg
  database_name  = "nexusflow"         
  base_capacity  = 8                   # 8 RPU minimum — perfect for dev
                                       # ⚠️ scale up to 32 for prod 
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  project_name = var.project_name

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# MODULE 9 — IAM
# Creates: All roles and policies for every service
# Depends on: S3, EKS (needs bucket names + cluster info)
# ══════════════════════════════════════════════════════════
module "iam" {
  source = "../../modules/iam"

  project_name     = var.project_name
  environment      = var.environment
  account_id       = data.aws_caller_identity.current.account_id # auto 
  region           = local.region
  lakehouse_bucket = module.s3.bucket_names["lakehouse"]
  artifacts_bucket = module.s3.bucket_names["artifacts"]
  logs_bucket      = module.s3.bucket_names["logs"]
  eks_cluster_name = module.eks.cluster_name
  eks_oidc_provider = replace(
    module.eks.oidc_provider_arn,
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/",
    ""
  )

  tags = local.tags
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# Printed to terminal after terraform apply completes.
# Also visible in Terraform Cloud UI under Outputs tab.
# ══════════════════════════════════════════════════════════
output "eks_cluster_name" {
  description = "EKS cluster name — use in aws eks update-kubeconfig"
  value       = module.eks.cluster_name
}

output "ecr_registry" {
  description = "ECR registry URL — use when pushing Docker images"
  value       = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

output "lakehouse_bucket" {
  description = "S3 lakehouse bucket name — bronze/silver/gold data lives here"
  value       = module.s3.bucket_names["lakehouse"]
}

output "msk_bootstrap_brokers" {
  description = "Kafka broker connection string — needed by producer and consumer"
  value       = module.msk.bootstrap_brokers_sasl_iam
  sensitive   = true # hidden in logs, visible in TF Cloud with permission
}

output "redshift_endpoint" {
  description = "Redshift Serverless endpoint — needed by dbt and FastAPI"
  value       = module.redshift.endpoint
  sensitive   = true
}

output "glue_catalog_id" {
  description = "Glue Data Catalog ID"
  value       = module.glue.catalog_id
}

output "emr_application_id" {
  description = "EMR Serverless application ID — needed by Airflow DAG"
  value       = module.emr.application_id
}

output "emr_execution_role_arn" {
  description = "IAM role ARN for EMR Spark jobs"
  value       = module.iam.emr_execution_role_arn
}

output "aws_account_id" {
  description = "Your AWS account ID — for reference"
  value       = data.aws_caller_identity.current.account_id
}
