# ============================================================
# NexusFlow — Dev Environment Variable Declarations
#
# variables and variable type declaration.
# Actual VALUES are in terraform.tfvars.
# ============================================================

variable "aws_region" {
  description = "AWS region to deploy all resources into"
  type        = string
  default     = "ca-central-1"  # Note: instance types may vary by region
}

variable "environment" {
  description = "Deployment environment label"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name used as prefix for all resource names"
  type        = string
  default     = "nexusflow" # ⚠️ change here if renamed, as it affected dependencies S3 bucket names, EKS cluster name, etc.
}

variable "team_name" {
  description = "Team name used in resource tags for cost allocation"
  type        = string
  default     = "Smit-data-engineering-portfolio" 
                                   
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16" # ⚠️ CHANGE if this conflicts with an existing
                               # Check: AWS Console → VPC → Your VPCs
}

variable "private_subnets" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
 
  # ⚠️ CHANGE if vpc_cidr changes — must be subsets of vpc_cidr
  # EKS nodes, MSK brokers, Redshift -> lives in private subnets
}

variable "public_subnets" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
 
  # ⚠️ CHANGE if vpc_cidr changes — must be subsets of vpc_cidr
  # NAT gateways and load balancers -> lives in public subnets
}

variable "eks_cluster_version" {
  description = "Kubernetes version for EKS cluster"
  type        = string
  default     = "1.35" # ⚠️ CHANGE to latest available,version deprecates faster (14 months from release approx)
                       # Check: aws eks describe-addon-versions --query 'addons[0].addonVersions'                       
}

variable "eks_node_instance_types" {
  description = "EC2 instance types for general EKS node group"
  type        = list(string)
  default     = ["m5.xlarge", "m5.2xlarge"]
  # ⚠️ CHANGE to ["t3.medium"] for minimal cost during initial setup/testing
  # ⚠️ CHANGE to ["m5.2xlarge","m5.4xlarge"] for heavier workloads
  # Multiple types listed = EKS picks cheapest available (Spot-friendly)
}
