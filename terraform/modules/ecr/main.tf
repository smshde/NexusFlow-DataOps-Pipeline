# ============================================================
# NexusFlow — ECR Module
# File: terraform/modules/ecr/main.tf
#
# Creates: One container registry per service
#          Lifecycle policy to keep storage costs low
# ============================================================

resource "aws_ecr_repository" "repos" {
  for_each = toset(var.repositories) # creates one repo per service name

  name                 = each.value # e.g. "nexusflow-datagen"
  image_tag_mutability = "MUTABLE"  # allows overwriting :latest tag
  # ⚠️ CHANGE to "IMMUTABLE" for prod
  # (forces unique tags per image)

  force_delete = true # allows terraform destroy to delete repo, even if images exist inside it

  image_scanning_configuration {
    scan_on_push = true # automatically scan for CVEs on push
  }

  encryption_configuration {
    encryption_type = "AES256" # encrypt images at rest
    # ⚠️ CHANGE to "KMS" for prod compliance
  }

  tags = var.tags
}

# ── LIFECYCLE POLICY ──────────────────────────────────────
# Automatically deletes old images to control storage cost.
# Keeps only the 10 most recent images per repository.
# At ~500MB per image, 10 images = ~5GB per repo = ~$0.50/mo

resource "aws_ecr_lifecycle_policy" "repos" {
  for_each   = aws_ecr_repository.repos
  repository = each.value.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images, expire older ones"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10 # ⚠️ CHANGE to 25 for prod (provides more rollback options)
      }
      action = { type = "expire" }
    }]
  })
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "repository_urls" {
  description = "Map of service name to ECR repository URL"
  value       = { for k, v in aws_ecr_repository.repos : k => v.repository_url }
}

output "registry_id" {
  description = "ECR registry ID (same as AWS account ID)"
  value       = length(aws_ecr_repository.repos) > 0 ? values(aws_ecr_repository.repos)[0].registry_id : ""
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "repositories" {
  type        = list(string)
  description = "List of ECR repository names to create"
}

variable "tags" {
  type    = map(string)
  default = {}
}
