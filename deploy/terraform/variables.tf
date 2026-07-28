variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "ap-south-1"
}

variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "inference-ledger"
}

variable "deployment_profile" {
  description = <<-EOT
    Which backing services AWS provides, versus which run inside the cluster.

    "minimal" — EKS + VPC only. Redpanda, Postgres and Redis run as pods via the
      Helm chart's bundled infra. The system is fully functional; you are simply
      operating the stateful parts yourself. Roughly 5x cheaper, and the right
      choice for a demo or a review environment.

    "full" — EKS + MSK Serverless + RDS Postgres + ElastiCache Redis. The
      managed-service deployment the chart's endpoint overrides exist for. What
      you would actually run in production, and what the cost table in README.md
      is measured against.

    The Helm values differ accordingly; `terraform output helm_values` prints the
    right ones for the profile you chose.
  EOT
  type        = string
  default     = "minimal"

  validation {
    condition     = contains(["minimal", "full"], var.deployment_profile)
    error_message = "deployment_profile must be \"minimal\" or \"full\"."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = <<-EOT
    Number of availability zones. Three is the floor for a production posture:
    MSK Serverless requires multi-AZ, and two AZs leave no headroom for an AZ
    outage during a node-group rollout.
  EOT
  type        = number
  default     = 3

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 4
    error_message = "az_count must be between 2 and 4."
  }
}

variable "kubernetes_version" {
  description = "EKS control-plane version."
  type        = string
  default     = "1.31"
}

variable "node_instance_types" {
  description = <<-EOT
    Instance types for the managed node group. The default is deliberately small:
    this workload is I/O-bound (proxying streams, consuming Kafka), not compute-
    bound, so paying for large nodes buys nothing.
  EOT
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_group_size" {
  description = "Managed node group scaling bounds."
  type = object({
    min     = number
    max     = number
    desired = number
  })
  default = {
    min     = 2
    max     = 5
    desired = 2
  }
}

variable "single_nat_gateway" {
  description = <<-EOT
    Run one NAT gateway instead of one per AZ. Saves roughly $32/month per AZ
    avoided, at the cost of making that one AZ a dependency for all egress.
    Correct for a demo; flip to false for production, where an AZ failure must
    not take out every node's outbound traffic.
  EOT
  type        = bool
  default     = true
}

# --- Managed services (deployment_profile = "full") ---

variable "rds" {
  description = "RDS Postgres sizing and durability for the settlement ledger."
  type = object({
    instance_class      = string
    allocated_storage   = number
    engine_version      = string
    multi_az            = bool
    deletion_protection = bool
    # Enhanced monitoring and Performance Insights are genuinely useful and
    # genuinely chargeable (CloudWatch metrics, PI storage beyond 7 days).
    # Exposed rather than hardcoded so production turns them on deliberately.
    enhanced_monitoring = bool
    performance_insights = bool
  })
  default = {
    instance_class    = "db.t4g.micro"
    allocated_storage = 20
    engine_version    = "17.2"
    # The demo defaults optimise for cost and frictionless teardown. Every one
    # of these should be inverted for anything holding real billing data — see
    # the production block in terraform.tfvars.example.
    multi_az             = false
    deletion_protection  = false
    enhanced_monitoring  = false
    performance_insights = false
  }
}

variable "elasticache" {
  description = "ElastiCache Redis sizing for idempotency keys and token buckets."
  type = object({
    node_type       = string
    engine_version  = string
    num_cache_nodes = number
  })
  default = {
    node_type      = "cache.t4g.micro"
    engine_version = "7.1"
    # Redis holds only ephemeral state that can be safely lost on failure
    # (in-flight idempotency leases, refillable token buckets), so a single node
    # is a defensible trade here in a way it would not be for the ledger.
    num_cache_nodes = 1
  }
}

variable "msk" {
  description = "MSK Serverless limits. Serverless has no broker sizing — you set quotas."
  type = object({
    max_throughput_mb_per_second = number
  })
  default = {
    max_throughput_mb_per_second = 10
  }
}

variable "cluster_public_access" {
  description = <<-EOT
    Expose the EKS API server publicly. Needed for kubectl from a laptop without
    a bastion or VPN, which is why it defaults on for a demo — but it is a real
    exposure. Restrict it with cluster_public_access_cidrs, or set false and
    reach the API from inside the VPC.
  EOT
  type        = bool
  default     = true
}

variable "cluster_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "tags" {
  description = "Extra tags merged into every resource."
  type        = map(string)
  default     = {}
}
