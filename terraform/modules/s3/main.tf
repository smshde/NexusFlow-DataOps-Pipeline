# ============================================================
# NexusFlow — S3 Module
# File: terraform/modules/s3/main.tf
#
# Creates: All S3 buckets for the lakehouse
#          bronze / silver / gold / artifacts / logs / athena
# ============================================================

# ── S3 BUCKETS ────────────────────────────────────────────
# Creates one bucket per entry in var.buckets map.
resource "aws_s3_bucket" "buckets" {
  for_each = var.buckets

  bucket = each.value.name # e.g. "nexusflow-dev-lakehouse"
                            # ⚠️ Must be globally unique — if creation fails
                            # with BucketAlreadyExists, add a random suffix
                            # to the name in terraform.tfvars
  force_destroy = true    # Allows terraform to destroy buckets even if contains objects/versioned objects
                          # Not safe for prod
  tags = merge(var.tags, {
    Name  = each.value.name
    Layer = each.key # bronze, silver, gold, etc.
  })
}

# ── VERSIONING ────────────────────────────────────────────
# Enabled on lakehouse and artifacts — allows time-travel
# and recovery from accidental deletes/overwrites.
resource "aws_s3_bucket_versioning" "buckets" {
  for_each = var.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  versioning_configuration {
    status = each.value.versioning ? "Enabled" : "Suspended"
  }
}

# ── ENCRYPTION ────────────────────────────────────────────
# AES256 = AWS-managed keys (free, sufficient for portfolio)
# ⚠️ CHANGE to aws:kms for prod compliance requirements
resource "aws_s3_bucket_server_side_encryption_configuration" "buckets" {
  for_each = var.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true # reduces KMS API costs
  }
}

# ── BLOCK ALL PUBLIC ACCESS ───────────────────────────────
# Never allow public access to data lake buckets.
resource "aws_s3_bucket_public_access_block" "buckets" {
  for_each = var.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# bucket object ownership -> to avoid error "InvalidBucketAclWithObjectOwnership"

resource "aws_s3_bucket_ownership_controls" "buckets" {
  for_each = var.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  rule {
    object_ownership = "BucketOwnerEnforced"
    # BucketOwnerEnforced = bucket owner owns all objects
    # disables ACLs entirely — the modern AWS standard
    # required for all buckets created after April 2023
  }

  depends_on = [aws_s3_bucket_public_access_block.buckets]
}


# ── LIFECYCLE POLICIES ────────────────────────────────────
# Automatically move old data to cheaper storage classes.
# Only applied to buckets with lifecycle_enabled = true.
#
# Storage class cost comparison (per GB/month):
#   STANDARD:       $0.023
#   STANDARD_IA:    $0.0125  (40% cheaper, min 30-day charge)
#   GLACIER:        $0.004   (83% cheaper, retrieval takes minutes)
resource "aws_s3_bucket_lifecycle_configuration" "buckets" {
  for_each = {
    for k, v in var.buckets : k => v if v.lifecycle_enabled
  }

  bucket = aws_s3_bucket.buckets[each.key].id

  rule {
    id     = "transition-to-cheaper-storage"
    status = "Enabled"
    
    filter {}
    # empty filter = apply rule to ALL objects in bucket
    # required by AWS provider 5.x 
    # Move to Infrequent Access after N days
    
    transition {
      days          = each.value.transition_days # 90 for lakehouse, 30 for logs
      storage_class = "STANDARD_IA"
    }

    # Move to Glacier after 3x transition_days
    transition {
      days          = each.value.transition_days * 3
      storage_class = "GLACIER"
    }

    # Clean up incomplete multipart uploads (saves cost)
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ── BUCKET NOTIFICATION (optional — for event-driven) ─────
# Uncomment in Sprint 2 when you want S3 events to trigger Lambda
# resource "aws_s3_bucket_notification" "lakehouse" {
#   bucket = aws_s3_bucket.buckets["lakehouse"].id
#   ...
# }

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "bucket_names" {
  description = "Map of bucket key to bucket name"
  value       = { for k, v in aws_s3_bucket.buckets : k => v.id }
}

output "bucket_arns" {
  description = "Map of bucket key to bucket ARN"
  value       = { for k, v in aws_s3_bucket.buckets : k => v.arn }
}

output "lakehouse_bucket_name" {
  description = "The main lakehouse bucket name (shortcut)"
  value       = aws_s3_bucket.buckets["lakehouse"].id
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

variable "buckets" {
  description = "Map of bucket configurations"
  type = map(object({
    name              = string
    versioning        = bool
    lifecycle_enabled = bool
    transition_days   = optional(number, 90)
  }))
}

variable "tags" {
  type    = map(string)
  default = {}
}
