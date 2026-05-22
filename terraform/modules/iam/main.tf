# ============================================================
# NexusFlow — IAM Module
#
# Creates: All IAM roles and policies for:
#   - EMR Serverless (Spark job execution)
#   - Airflow (via IRSA — IAM Roles for Service Accounts)
#   - dbt runner (Redshift + S3 access)
#   - Data generator / API service accounts
# ============================================================

data "aws_caller_identity" "current" {}

# ══════════════════════════════════════════════════════════
# ROLE 1 — EMR EXECUTION ROLE
# Assumed by EMR Serverless when running Spark jobs.
# Needs: S3 read/write, Glue catalog, CloudWatch logs.
# ══════════════════════════════════════════════════════════
resource "aws_iam_role" "emr_execution" {
  name = "${var.project_name}-${var.environment}-emr-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "emr_full_access" {
  name = "${var.project_name}-${var.environment}-emr-policy"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.lakehouse_bucket}",
          "arn:aws:s3:::${var.lakehouse_bucket}/*",
          "arn:aws:s3:::${var.artifacts_bucket}",
          "arn:aws:s3:::${var.artifacts_bucket}/*",
          "arn:aws:s3:::${var.logs_bucket}",
          "arn:aws:s3:::${var.logs_bucket}/*"
        ]
      },
      {
        Sid    = "GlueCatalogReadWrite"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",    "glue:GetDatabases",
          "glue:GetTable",       "glue:GetTables",
          "glue:CreateTable",    "glue:UpdateTable",
          "glue:GetPartition",   "glue:GetPartitions",
          "glue:BatchCreatePartition", "glue:BatchGetPartition"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "CloudWatchLogging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/emr-serverless/*"]
      }
    ]
  })
}

# ══════════════════════════════════════════════════════════
# ROLE 2 — AIRFLOW IRSA ROLE
# Assumed by the Airflow scheduler pod inside EKS.
# Uses IRSA (Web Identity) — no static credentials needed.
# Needs: EMR submit, S3 read, Secrets Manager, Glue.
# ══════════════════════════════════════════════════════════
resource "aws_iam_role" "airflow_irsa" {
  name = "${var.project_name}-${var.environment}-airflow-irsa"

  # Web Identity trust — allows the Airflow K8s service account
  # to assume this role using the EKS OIDC token.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRoleWithWebIdentity"
      Principal = {
        Federated = "arn:aws:iam::${var.account_id}:oidc-provider/${var.eks_oidc_provider}"
      }
      Condition = {
        StringEquals = {
          # Only the Airflow scheduler service account can assume this role
          "${var.eks_oidc_provider}:sub" = "system:serviceaccount:airflow:airflow-scheduler"
          "${var.eks_oidc_provider}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "airflow_permissions" {
  name = "${var.project_name}-${var.environment}-airflow-policy"
  role = aws_iam_role.airflow_irsa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EMRServerlessJobSubmit"
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns",
          "emr-serverless:GetApplication"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "IAMPassRoleForEMR"
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [aws_iam_role.emr_execution.arn]
      },
      {
        Sid    = "S3PipelineAccess"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.lakehouse_bucket}",
          "arn:aws:s3:::${var.lakehouse_bucket}/*",
          "arn:aws:s3:::${var.artifacts_bucket}",
          "arn:aws:s3:::${var.artifacts_bucket}/*"
        ]
      },
      {
        Sid    = "SecretsManagerAccess"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:nexusflow/*"
        ]
        # ⚠️ CHANGE "nexusflow" prefix if project name renamed
      },
      {
        Sid    = "GlueReadAccess"
        Effect = "Allow"
        Action = ["glue:GetCrawler", "glue:StartCrawler",
                  "glue:GetCrawlerMetrics", "glue:GetDatabase",
                  "glue:GetTable"]
        Resource = ["*"]
      }
    ]
  })
}

# ══════════════════════════════════════════════════════════
# ROLE 3 — DBT RUNNER IRSA ROLE
# Assumed by the dbt CronJob pod inside EKS.
# Needs: Redshift Data API, S3 read, Secrets Manager.
# ══════════════════════════════════════════════════════════
resource "aws_iam_role" "dbt_irsa" {
  name = "${var.project_name}-${var.environment}-dbt-irsa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRoleWithWebIdentity"
      Principal = {
        Federated = "arn:aws:iam::${var.account_id}:oidc-provider/${var.eks_oidc_provider}"
      }
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider}:sub" = "system:serviceaccount:nexusflow:nexusflow-dbt-sa"
          "${var.eks_oidc_provider}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dbt_permissions" {
  name = "${var.project_name}-${var.environment}-dbt-policy"
  role = aws_iam_role.dbt_irsa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RedshiftDataAPI"
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:DescribeStatement",
          "redshift-data:ListStatements",
          "redshift-serverless:GetCredentials"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "SecretsManagerAccess"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:nexusflow/*"
        ]
      },
      {
        Sid    = "S3ReadForSeeds"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.lakehouse_bucket}",
          "arn:aws:s3:::${var.lakehouse_bucket}/*"
        ]
      }
    ]
  })
}

# ══════════════════════════════════════════════════════════
# ROLE 4 — API / DATAGEN SERVICE ROLE
# Assumed by FastAPI and datagen pods via IRSA.
# Needs: S3 read (API), MSK produce (datagen), Secrets Manager.
# ══════════════════════════════════════════════════════════
resource "aws_iam_role" "app_irsa" {
  name = "${var.project_name}-${var.environment}-app-irsa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRoleWithWebIdentity"
      Principal = {
        Federated = "arn:aws:iam::${var.account_id}:oidc-provider/${var.eks_oidc_provider}"
      }
      Condition = {
        StringLike = {
          # Allow multiple service accounts to assume this role
          "${var.eks_oidc_provider}:sub" = "system:serviceaccount:nexusflow:nexusflow-*"
        }
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "app_permissions" {
  name = "${var.project_name}-${var.environment}-app-policy"
  role = aws_iam_role.app_irsa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MSKConnect"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic"
        ]
        Resource = ["*"]
        # ⚠️ Tighten to specific MSK cluster ARN for prod
      },
      {
        Sid    = "SecretsManagerAccess"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:nexusflow/*"
        ]
      },
      {
        Sid    = "RedshiftQueryAccess"
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:GetStatementResult",
          "redshift-serverless:GetCredentials"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "emr_execution_role_arn" {
  description = "EMR execution role ARN — passed to EMR module and Airflow DAG"
  value       = aws_iam_role.emr_execution.arn
}

output "airflow_irsa_role_arn" {
  description = "Airflow IRSA role ARN — annotate airflow-scheduler service account"
  value       = aws_iam_role.airflow_irsa.arn
}

output "dbt_irsa_role_arn" {
  description = "dbt IRSA role ARN — annotate nexusflow-dbt-sa service account"
  value       = aws_iam_role.dbt_irsa.arn
}

output "app_irsa_role_arn" {
  description = "App IRSA role ARN — annotate API and datagen service accounts"
  value       = aws_iam_role.app_irsa.arn
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "project_name"      { type = string }
variable "environment"       { type = string }
variable "account_id"        { type = string }
variable "region"            { type = string }
variable "lakehouse_bucket"  { type = string }
variable "artifacts_bucket"  { type = string }
variable "logs_bucket"       { type = string }
variable "eks_cluster_name"  { type = string }

variable "eks_oidc_provider" {
  type        = string
  description = "EKS OIDC provider URL (without https://)"
  default     = ""
  # Auto-populated from EKS module output in main.tf
  # Format: oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B7
}

variable "tags" {
  type    = map(string)
  default = {}
}
