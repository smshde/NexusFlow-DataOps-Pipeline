# ============================================================
# NexusFlow — Glue Module
#
# Creates: Glue Data Catalog databases (bronze/silver/gold)
#          Glue Crawlers for automatic schema discovery
#          IAM role for crawlers to access S3
# ============================================================

# ── GLUE CATALOG DATABASES ────────────────────────────────
# One database per medallion layer.
# These appear in AWS Glue → Data Catalog → Databases.
# Also queryable via Amazon Athena and Redshift Spectrum.
resource "aws_glue_catalog_database" "layers" {
  for_each    = toset(var.database_names) # ["bronze", "silver", "gold"]

  name        = "${var.project_name}_${var.environment}_${each.value}"
  # e.g. nexusflow_dev_bronze
  # ⚠️ Glue database names cannot contain hyphens — underscores only

  description = "NexusFlow ${each.value} layer — ${var.environment} environment"
}

# ── IAM ROLE FOR GLUE CRAWLERS ────────────────────────────
resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-${var.environment}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = var.tags
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Add S3 read access so crawlers can discover schemas
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-${var.environment}-glue-s3"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",   # needed to write crawler metadata
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.lakehouse_bucket}",
          "arn:aws:s3:::${var.lakehouse_bucket}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream",
                    "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:*:/aws-glue/*"]
      }
    ]
  })
}

# ── GLUE CRAWLERS ─────────────────────────────────────────
# One crawler per layer — runs on schedule to update schema
# automatically when new Parquet files land in S3.
resource "aws_glue_crawler" "layers" {
  for_each = toset(var.database_names)

  name          = "${var.project_name}-${var.environment}-${each.value}-crawler"
  database_name = aws_glue_catalog_database.layers[each.value].name
  role          = aws_iam_role.glue_crawler.arn

  # Cron: run every 6 hours
  # ⚠️ "cron(0 */2 * * ? *)" for 2-hour schedule in prod
  # ⚠️ CHANGE to "" (empty) to disable automatic schedule and run manually
  # schedule = "cron(0 4 * * ? *)" # once daily at 04:00 UTC,Runs after Spark job completes (03:00 start + ~30min processing) # schedule removed: on-demand via Airflow-only
  # schedule only needs to be included if not managed by Airflow and in such case recrawl_policy needed.

  # Points crawler at the layer's S3 prefix
  s3_target {
    path = "s3://${var.lakehouse_bucket}/${each.value}/"
    # e.g. s3://nexusflow-dev-lakehouse/bronze/
    #      s3://nexusflow-dev-lakehouse/silver/
    #      s3://nexusflow-dev-lakehouse/gold/
  }

  # What happens when schema changes
  schema_change_policy {
    update_behavior = "LOG" # update schema on changes
    delete_behavior = "LOG"               # log but don't delete on removals
                                          # ⚠️ CHANGE to "DEPRECATE_IN_DATABASE" for prod safety
  }

  tags = var.tags
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "catalog_id" {
  description = "Glue Data Catalog ID (same as AWS account ID)"
  value       = aws_glue_catalog_database.layers[var.database_names[0]].catalog_id
}

output "database_names" {
  description = "Map of layer to Glue database name"
  value       = { for k, v in aws_glue_catalog_database.layers : k => v.name }
}

output "crawler_names" {
  description = "Map of layer to Glue crawler name"
  value       = { for k, v in aws_glue_crawler.layers : k => v.name }
}

output "crawler_role_arn" {
  description = "IAM role ARN used by Glue crawlers"
  value       = aws_iam_role.glue_crawler.arn
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "project_name" {
  type        = string
  description = "Project name prefix"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "lakehouse_bucket" {
  type        = string
  description = "S3 lakehouse bucket name — passed from S3 module output"
}

variable "database_names" {
  type        = list(string)
  description = "List of Glue database names to create (one per layer)"
  default     = ["bronze", "silver", "gold"]
}

variable "tags" {
  type    = map(string)
  default = {}
}
