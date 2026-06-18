# ============================================================
# NexusFlow — MSK (Managed Kafka) Module
#
# Creates: Kafka cluster with IAM auth, encryption,
#          CloudWatch + S3 logging, Prometheus monitoring
# ============================================================

# ── KMS KEY FOR MSK ENCRYPTION ────────────────────────────
resource "aws_kms_key" "msk" {
  description             = "MSK cluster encryption key — ${var.cluster_name}"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = var.tags
}

# ── CLOUDWATCH LOG GROUP ──────────────────────────────────
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.cluster_name}"
  retention_in_days = 30 # ⚠️ CHANGE to 90 for prod

  tags = var.tags
}

# ── MSK CONFIGURATION ────────────────────────────────────
# Custom broker settings — tuned for e-commerce event volume.
resource "aws_msk_configuration" "main" {
  kafka_versions = [var.kafka_version] # ["3.6.0"]
  name           = "${var.cluster_name}-config"

  server_properties = <<PROPERTIES
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.retention.hours=168
log.segment.bytes=1073741824
default.replication.factor=2
min.insync.replicas=1
num.partitions=6
compression.type=lz4
group.min.session.timeout.ms=6000
group.max.session.timeout.ms=300000
auto.create.topics.enable=false
message.max.bytes=10485760
PROPERTIES
}

# ── SECURITY GROUP ────────────────────────────────────────
resource "aws_security_group" "msk" {
  name        = "${var.cluster_name}-msk-sg"
  description = "MSK Kafka cluster security group"
  vpc_id      = var.vpc_id

  # Kafka TLS (used by most clients)
  ingress {
    description = "Kafka TLS from within VPC"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"] # ⚠️ CHANGE if your VPC CIDR is different
  }

  # Kafka IAM/SASL (used by producers with AWS IAM auth)
  ingress {
    description = "Kafka IAM auth TLS from within VPC"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"] # ⚠️ CHANGE if your VPC CIDR is different
  }

  # Prometheus scrape endpoints (for Grafana dashboards)
  ingress {
    description = "Prometheus JMX + Node exporter"
    from_port   = 11001
    to_port     = 11002
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"] # ⚠️ CHANGE if your VPC CIDR is different
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-msk-sg"
  })
}

# ── MSK CLUSTER ───────────────────────────────────────────
resource "aws_msk_cluster" "main" {
  cluster_name           = var.cluster_name  # nexusflow-dev-kafka
  kafka_version          = var.kafka_version # 3.6.0
  number_of_broker_nodes = var.broker_count  # 2 for dev, 3 for prod

  broker_node_group_info {
    instance_type  = var.instance_type                          # kafka.t3.small for dev
    client_subnets = slice(var.subnet_ids, 0, var.broker_count) # slice takes only as many subnets as brokers
    storage_info {
      ebs_storage_info {
        volume_size = var.storage_gb # 100 GB per broker for dev
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS" # force TLS between client and broker
      in_cluster    = true  # encrypt inter-broker traffic
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
  }

  # IAM authentication — no username/password needed
  # Kafka clients authenticate using their AWS IAM role
  client_authentication {
    sasl {
      iam = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  # ── LOGGING ─────────────────────────────────────────────
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
      s3 {
        enabled = true
        bucket  = var.logs_bucket # auto from S3 module
        prefix  = "kafka-broker-logs/"
      }
    }
  }

  # ── PROMETHEUS MONITORING ────────────────────────────────
  # Exposes JMX and Node metrics for Grafana dashboards
  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  tags = var.tags
}

# ══════════════════════════════════════════════════════════
# OUTPUTS
# ══════════════════════════════════════════════════════════
output "cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

output "bootstrap_brokers_tls" {
  description = "Kafka broker connection string (TLS)"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
  sensitive   = true
}

output "bootstrap_brokers_sasl_iam" {
  description = "Kafka broker connection string (IAM auth) — use this"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "zookeeper_connect_string" {
  description = "ZooKeeper connection string (for admin tasks)"
  value       = aws_msk_cluster.main.zookeeper_connect_string
  sensitive   = true
}

output "security_group_id" {
  description = "MSK security group ID"
  value       = aws_security_group.msk.id
}

# ══════════════════════════════════════════════════════════
# VARIABLES
# ══════════════════════════════════════════════════════════
variable "cluster_name" {
  type        = string
  description = "MSK cluster name"
}

variable "kafka_version" {
  type        = string
  description = "Kafka version"
  default     = "3.6.0"
}

variable "instance_type" {
  type        = string
  description = "MSK broker instance type"
  default     = "kafka.t3.small" # cheapest — fine for dev
}

variable "broker_count" {
  type        = number
  description = "Number of Kafka brokers"
  default     = 2 # minimum; must be multiple of AZ count for prod
}

variable "vpc_id" {
  type        = string
  description = "VPC ID for security group"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs — one per broker node"
}

variable "storage_gb" {
  type        = number
  description = "EBS storage per broker in GB"
  default     = 100
}

variable "logs_bucket" {
  type        = string
  description = "S3 bucket name for MSK broker logs"
  # no default — must be explicitly passed from main.tf
}

variable "topics" {
  type = list(object({
    name        = string
    partitions  = number
    replication = number
  }))
  default     = []
  description = "Kafka topics to document (actual creation via CLI/Lambda)"
}

variable "tags" {
  type    = map(string)
  default = {}
}
