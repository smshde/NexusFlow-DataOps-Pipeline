# ============================================================
# NexusFlow — Dev Environment Actual Values
#
# These are the REAL values passed to variables.tf.
# This file is read automatically by terraform plan/apply.
#
# ⚠️ Be careful befoer committing this file with personal or organizational config values to public GitHub.
# ============================================================

# ── CORE ──────────────────────────────────────────────────
aws_region   = "ca-central-1"
environment  = "dev"
project_name = "nexusflow"
team_name    = "smit-data-engineering-portfolio"

# ── NETWORK ───────────────────────────────────────────────
# check if 10.0.0.0/16 conflicts with an existing VPC in AWS account.
vpc_cidr        = "10.0.0.0/16"
private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

# ── EKS ───────────────────────────────────────────────────
eks_cluster_version     = "1.35"
eks_node_instance_types = ["m5.xlarge", "m5.2xlarge"]

# set, instance type = ["t3.medium"],for initial trial,low cost
# set, m5 for full demo.
# m5.2xlarge = $$0.04608/hr, m5.xlarge = $$0.02304/hr , t3.medium = $0.00499/hr 