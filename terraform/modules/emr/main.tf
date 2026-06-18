# ============================================================
# NexusFlow — EMR Serverless Module
#
# Creates: EMR Serverless application for PySpark jobs
#
# Key benefit: Pay ONLY when a Spark job is running.
# Idle cost = $0. Auto-stops after 15 min idle.
# ============================================================

# ── SECURITY GROUP FOR EMR ────────────────────────────────
# Controls network access for EMR Serverless workers.
resource "aws_security_group" "emr" {
  name        = "${var.project_name}-emr-sg"
  description = "EMR Serverless security group"
  vpc_id      = data.aws_subnet.selected.vpc_id

  # EMR workers need outbound access to:
  # - S3 (read bronze, write silver)
  # - Glue (schema registry)
  # - CloudWatch (logging)
  egress {
    description = "Allow all outbound for S3/Glue/CloudWatch access"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-emr-sg"
  })
}

# Look up VPC ID from the subnet (avoids needing vpc_id variable)
data "aws_subnet" "selected" {
  id = var.subnet_id
}

# ── EMR SERVERLESS APPLICATION ────────────────────────────
resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark"
  release_label = "emr-7.13.0" # ⚠️ can be changed to latest version
  # Check: aws emr-serverless list-applications
  type = "SPARK"

  # ── PRE-INITIALIZED CAPACITY ────────────────────────────
  # Keeps a small pool of workers warm to reduce cold-start time.
  # For demo: comment these blocks out to reduce cost to zero when idle.
  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1 # ⚠️ CHANGE to 0 for pure pay-per-use (slower/cold start)
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "8 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"
    initial_capacity_config {
      worker_count = 2 # ⚠️ CHANGE to 0 for pure pay-per-use
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
        disk   = "100 GB"
      }
    }
  }

  # ── MAXIMUM CAPACITY ────────────────────────────────────
  # Hard ceiling on resources — prevents runaway costs.
  maximum_capacity {
    cpu    = "16 vCPU" # ⚠️ Demo optimized    16 vCPU    64 GB   200 GB 
    memory = "64 GB"   # ⚠️ Dev comfortable   32 vCPU   128 GB   400 GB
    disk   = "300 GB"  # ⚠️ Prod realistic    80 vCPU   320 GB   800 GB
  }

  # ── AUTO START ──────────────────────────────────────────
  # Application starts automatically when a job is submitted.
  auto_start_configuration {
    enabled = true
  }

  # ── AUTO STOP ───────────────────────────────────────────
  # Shuts down after 10 min of idle — KEY for cost control.
  # ⚠️ 05 min for aggressive cost saving
  # ⚠️ 60 min if warm workers needed for longer
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 10
  }

  # ── NETWORK CONFIG ──────────────────────────────────────
  network_configuration {
    subnet_ids         = [var.subnet_id]
    security_group_ids = [aws_security_group.emr.id]
  }

  tags = var.tags
}

# ── CLOUDWATCH LOG GROUP FOR SPARK ────────────────────────
resource "aws_cloudwatch_log_group" "emr_spark" {
  name              = "/aws/emr-serverless/${var.project_name}/spark"
  retention_in_days = 7 # ⚠️ CHANGE +- days as reuired/ environment

  tags = var.tags
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "application_id" {
  description = "EMR Serverless application ID — used by Airflow DAG"
  value       = aws_emrserverless_application.spark.id
}

output "application_arn" {
  description = "EMR Serverless application ARN"
  value       = aws_emrserverless_application.spark.arn
}

output "security_group_id" {
  description = "EMR security group ID"
  value       = aws_security_group.emr.id
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "project_name" {
  type        = string
  description = "Project name for resource naming"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "subnet_id" {
  type        = string
  description = "Single subnet ID for EMR workers"
  # Use first private subnet: module.vpc.private_subnets[0]
}

variable "logs_bucket" {
  type        = string
  description = "S3 bucket for EMR job logs"
}

variable "artifacts_bucket" {
  type        = string
  description = "S3 bucket for Spark scripts and JARs"
}

variable "tags" {
  type    = map(string)
  default = {}
}
