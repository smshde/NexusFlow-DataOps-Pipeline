# ============================================================
# NexusFlow — VPC Module
#
# Creates: VPC, public subnets, private subnets,
#          Internet Gateway, NAT Gateway, Route Tables
# ============================================================

# ── VPC ───────────────────────────────────────────────────
resource "aws_vpc" "main" {
  cidr_block           = var.cidr                 # 10.0.0.0/16
  enable_dns_hostnames = var.enable_dns_hostnames # true — required for EKS
  enable_dns_support   = var.enable_dns_support   # true — required for EKS

  tags = merge(var.tags, {
    Name = "${var.name}-vpc" # nexusflow-dev-vpc
  })
}

# ── INTERNET GATEWAY ──────────────────────────────────────
# Required for public subnets to reach the internet.
# Used by: NAT Gateway, Load Balancers
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.tags, {
    Name = "${var.name}-igw" # nexusflow-dev-igw
  })
}

# ── PUBLIC SUBNETS ────────────────────────────────────────
# One per availability zone.
# Used by: NAT Gateways, Load Balancers
# Note: EKS nodes do NOT go in public subnets.
resource "aws_subnet" "public" {
  count = length(var.public_subnets)

  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnets[count.index]
  availability_zone       = var.azs[count.index]
  map_public_ip_on_launch = true # instances get public IP on launch

  tags = merge(var.tags, {
    Name                     = "${var.name}-public-${var.azs[count.index]}"
    "kubernetes.io/role/elb" = "1" # tells EKS to use these for external LBs
  })
}

# ── PRIVATE SUBNETS ───────────────────────────────────────
# One per availability zone.
# Used by: EKS nodes, MSK brokers, Redshift, EMR
# No direct internet access — traffic goes via NAT Gateway.
resource "aws_subnet" "private" {
  count = length(var.private_subnets)

  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnets[count.index]
  availability_zone = var.azs[count.index]

  tags = merge(var.tags, {
    Name                              = "${var.name}-private-${var.azs[count.index]}"
    "kubernetes.io/role/internal-elb" = "1" # tells EKS to use these for internal LBs
  })
}

# ── ELASTIC IPs FOR NAT GATEWAYS ─────────────────────────
# Each NAT gateway needs a static public IP.
# single_nat_gateway=true in dev means 1 EIP, 3 in prod.
resource "aws_eip" "nat" {
  count  = var.single_nat_gateway ? 1 : length(var.azs)
  domain = "vpc"

  tags = merge(var.tags, {
    Name = "${var.name}-nat-eip-${count.index}"
  })

  depends_on = [aws_internet_gateway.main]
}

# ── NAT GATEWAYS ──────────────────────────────────────────
# Allows private subnet resources (EKS nodes, MSK) to reach
# the internet for package downloads, API calls etc.
# Dev: 1 NAT gateway (cheaper, single point of failure OK)
# Prod: 3 NAT gateways (one per AZ for high availability)
resource "aws_nat_gateway" "main" {
  count = var.enable_nat_gateway ? (
    var.single_nat_gateway ? 1 : length(var.azs)
  ) : 0

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id # NAT lives in public subnet

  tags = merge(var.tags, {
    Name = "${var.name}-nat-${count.index}"
  })

  depends_on = [aws_internet_gateway.main]
}

# ── PUBLIC ROUTE TABLE ────────────────────────────────────
# Routes all outbound traffic from public subnets to IGW.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(var.tags, {
    Name = "${var.name}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  count          = length(var.public_subnets)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ── PRIVATE ROUTE TABLES ──────────────────────────────────
# Routes outbound traffic from private subnets to NAT Gateway.
# Dev: 1 route table (all private subnets share same NAT)
# Prod: 3 route tables (each AZ has its own NAT for resilience)
resource "aws_route_table" "private" {
  count  = var.single_nat_gateway ? 1 : length(var.azs)
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[var.single_nat_gateway ? 0 : count.index].id
  }

  tags = merge(var.tags, {
    Name = "${var.name}-private-rt-${count.index}"
  })
}

resource "aws_route_table_association" "private" {
  count          = length(var.private_subnets)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[var.single_nat_gateway ? 0 : count.index].id
}

# ══════════════════════════════════════════════════════════
# OUTPUTS — passed back to environments/dev/main.tf
# ══════════════════════════════════════════════════════════
output "vpc_id" {
  description = "VPC ID — used by EKS, MSK, Redshift modules"
  value       = aws_vpc.main.id
}

output "public_subnets" {
  description = "Public subnet IDs — used by load balancers"
  value       = aws_subnet.public[*].id
}

output "private_subnets" {
  description = "Private subnet IDs — used by EKS, MSK, Redshift, EMR"
  value       = aws_subnet.private[*].id
}

output "nat_gateway_ips" {
  description = "NAT Gateway public IPs — for whitelisting if needed"
  value       = aws_eip.nat[*].public_ip
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

# ══════════════════════════════════════════════════════════
# VARIABLES — values come from environments/dev/main.tf
# Do not change defaults here — change in terraform.tfvars
# ══════════════════════════════════════════════════════════
variable "name"    { type = string }
variable "cidr"    { type = string }
variable "azs"     { type = list(string) }

variable "public_subnets" {
  type        = list(string)
  description = "CIDR blocks for public subnets"
}

variable "private_subnets" {
  type        = list(string)
  description = "CIDR blocks for private subnets"
}

variable "enable_nat_gateway" {
  type    = bool
  default = true
}

variable "single_nat_gateway" {
  type    = bool
  default = true # true for dev (cost saving), false for prod
}

variable "enable_dns_hostnames" {
  type    = bool
  default = true
}

variable "enable_dns_support" {
  type    = bool
  default = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
