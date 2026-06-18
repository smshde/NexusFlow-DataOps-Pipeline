# ============================================================
# NexusFlow — Redshift Serverless Module
#
# Creates: Redshift Serverless namespace + workgroup
#          Admin credentials in Secrets Manager
#          IAM role for S3 + Glue access
#
# Cost model: Charged per RPU-second only when queries run.
# Idle = $0. Perfect for portfolio/demo.
# ============================================================


# ── KMS KEY ───────────────────────────────────────────────
resource "aws_kms_key" "redshift" {
  description             = "Redshift Serverless encryption — ${var.namespace_name}"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = var.tags
}

# ── SECURITY GROUP ────────────────────────────────────────
resource "aws_security_group" "redshift" {
  name        = "${var.workgroup_name}-sg"
  description = "Redshift Serverless security group"
  vpc_id      = var.vpc_id

  # Allow Redshift connections from within VPC
  # Port 5439 = Redshift (PostgreSQL-compatible)
  ingress {
    description = "Redshift port from within VPC"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"] # ⚠️ CHANGE if vpc_cidr is different
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.workgroup_name}-sg"
  })
}

# ── IAM ROLE FOR REDSHIFT ─────────────────────────────────
resource "aws_iam_role" "redshift" {
  name = "${var.workgroup_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift-serverless.amazonaws.com" }
    }]
  })

  tags = var.tags
}

# S3 access for COPY commands and Spectrum queries
resource "aws_iam_role_policy" "redshift_s3" {
  name = "${var.workgroup_name}-s3-policy"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        # ⚠️ CHANGE var.project_name if project name is renamed ->>> uses var.project_name instead of hardcoded "nexusflow"
        Resource = [
          "arn:aws:s3:::${var.project_name}-*",
          "arn:aws:s3:::${var.project_name}-*/*"
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# ── REDSHIFT SERVERLESS NAMESPACE ────────────────────────
# Namespace = the logical container for databases and users.
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name        = var.namespace_name
  db_name               = var.database_name
  admin_username        = "nexusflow_admin"
  manage_admin_password = true

  # AWS auto-generates password and stores it in Secrets Manager , No random_password resource needed anymore
  # connection details can be retrieved from : AWS Console → Secrets Manager → redshift!nexusflow-dev-ns-nexusflow_admin

  default_iam_role_arn = aws_iam_role.redshift.arn
  iam_roles            = [aws_iam_role.redshift.arn]
  kms_key_id           = aws_kms_key.redshift.arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]

  tags = var.tags
}

# ── REDSHIFT SERVERLESS WORKGROUP ─────────────────────────
# Workgroup = the compute layer. RPU = Redshift Processing Units.
# 8 RPU minimum. Cost charged per RPU-second of actual query time.
resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = var.workgroup_name # nexusflow-dev-wg
  base_capacity  = var.base_capacity  # 8 RPU for dev
  # ⚠️ CHANGE to 32 for prod
  # ⚠️ CHANGE to 128 for heavy analytics
  # Query configuration
  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }

  config_parameter {
    parameter_key   = "max_query_execution_time"
    parameter_value = "3600" # 1 hours max query time - dev cost control
    # ⚠️ CHANGE to 14400 (4hr) or more if needed 
  }

  config_parameter {
    parameter_key   = "datestyle"
    parameter_value = "ISO, MDY"
  }

  security_group_ids  = [aws_security_group.redshift.id]
  subnet_ids          = var.subnet_ids
  publicly_accessible = false # never expose Redshift to internet
  # access via VPC only

  tags = var.tags
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "endpoint" {
  description = "Redshift Serverless endpoint hostname"
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].address
  sensitive   = true
}

output "workgroup_arn" {
  description = "Redshift workgroup ARN"
  value       = aws_redshiftserverless_workgroup.main.arn
}

output "namespace_id" {
  description = "Redshift namespace ID"
  value       = aws_redshiftserverless_namespace.main.id
}

output "admin_secret_arn" {
  description = "Secrets Manager ARN — auto-created by manage_admin_password"
  value       = aws_redshiftserverless_namespace.main.admin_password_secret_arn
  # AWS exposes the auto-created secret ARN directly on the namespace resource
}

output "iam_role_arn" {
  description = "IAM role ARN attached to Redshift"
  value       = aws_iam_role.redshift.arn
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "namespace_name" {
  type        = string
  description = "Redshift Serverless namespace name"
}

variable "workgroup_name" {
  type        = string
  description = "Redshift Serverless workgroup name"
}

variable "base_capacity" {
  type        = number
  description = "Base RPU capacity (minimum 8)"
  default     = 8
}

variable "database_name" {
  type        = string
  description = "Default database name"
  default     = "nexusflow"
  # ⚠️ CHANGE if different database name is needed
}

variable "vpc_id" {
  type        = string
  description = "VPC ID for security group"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs for Redshift"
}

variable "project_name" {
  type        = string
  description = "Project name — used in S3 resource ARN pattern"
  default     = "nexusflow"
  # ⚠️ CHANGE var.project_name if project name is renamed ->>> uses var.project_name instead of hardcoded "nexusflow"
}

variable "tags" {
  type    = map(string)
  default = {}
}
