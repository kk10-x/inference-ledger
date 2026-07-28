data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # One switch drives every managed service, so a profile can never be half
  # applied — you cannot end up paying for MSK while the chart still points at
  # an in-cluster Redpanda.
  managed = var.deployment_profile == "full"

  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  tags = merge(
    {
      Project    = var.name
      ManagedBy  = "terraform"
      Profile    = var.deployment_profile
      Repository = "github.com/kk10-x/inference-ledger"
    },
    var.tags,
  )

  # Kubernetes service account the gateway and reconciler run as. IRSA binds an
  # IAM role to this name, so the pods get MSK credentials without a static key
  # ever existing.
  service_account_namespace = "default"
  service_account_name      = "inference-ledger"
}
