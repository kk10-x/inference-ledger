# Security posture that must not regress.
#
# Each assertion here corresponds to a mistake that is easy to make in a hurry
# and expensive to notice later: a publicly reachable database, an unencrypted
# ledger, a cache that silently discards the state it exists to hold.

mock_provider "aws" {}
mock_provider "helm" {}
mock_provider "random" {}

override_data {
  target = data.aws_availability_zones.available
  values = {
    names = ["ap-south-1a", "ap-south-1b", "ap-south-1c"]
  }
}

override_module {
  target = module.vpc
  outputs = {
    vpc_id                  = "vpc-0mock"
    private_subnets         = ["subnet-a", "subnet-b", "subnet-c"]
    public_subnets          = ["subnet-x", "subnet-y", "subnet-z"]
    private_route_table_ids = ["rtb-mock"]
  }
}

override_module {
  target = module.eks
  outputs = {
    cluster_name                       = "inference-ledger"
    cluster_endpoint                   = "https://mock.eks.amazonaws.com"
    cluster_certificate_authority_data = "bW9jaw=="
    node_security_group_id             = "sg-0mock"
    oidc_provider_arn                  = "arn:aws:iam::123456789012:oidc-provider/mock"
  }
}

run "managed_data_stores_are_private_and_encrypted" {
  command = plan

  variables {
    deployment_profile = "full"
  }

  override_module {
    target = module.irsa[0]
    outputs = {
      iam_role_arn = "arn:aws:iam::123456789012:role/inference-ledger-app"
    }
  }

  assert {
    condition     = aws_db_instance.this[0].publicly_accessible == false
    error_message = "the settlement ledger must never be reachable from the internet"
  }

  assert {
    condition     = aws_db_instance.this[0].storage_encrypted == true
    error_message = "the settlement ledger holds billing data and must be encrypted at rest"
  }

  assert {
    condition     = aws_db_instance.this[0].backup_retention_period >= 7
    error_message = "the ledger is the system of record; keep at least 7 days of backups"
  }

  # Redis holds idempotency claims and token buckets. Evicting them would hand
  # out free tokens and re-admit duplicate requests — silently, which is worse
  # than failing.
  assert {
    condition = anytrue([
      for p in aws_elasticache_parameter_group.this[0].parameter :
      p.name == "maxmemory-policy" && p.value == "noeviction"
    ])
    error_message = "Redis must not evict idempotency keys or token buckets under memory pressure"
  }
}

run "msk_requires_iam_authentication" {
  command = plan

  variables {
    deployment_profile = "full"
  }

  override_module {
    target = module.irsa[0]
    outputs = {
      iam_role_arn = "arn:aws:iam::123456789012:role/inference-ledger-app"
    }
  }

  # MSK Serverless supports IAM only, so this is really a guard against someone
  # "simplifying" it later and expecting a SASL password to work.
  assert {
    condition     = aws_msk_serverless_cluster.this[0].client_authentication[0].sasl[0].iam[0].enabled
    error_message = "MSK must use IAM auth; there is no password-based alternative on Serverless"
  }
}

run "database_ingress_is_restricted_to_the_cluster" {
  command = plan

  variables {
    deployment_profile = "full"
  }

  override_module {
    target = module.irsa[0]
    outputs = {
      iam_role_arn = "arn:aws:iam::123456789012:role/inference-ledger-app"
    }
  }

  # Security-group source, not CIDR: the database should be reachable from the
  # node group and nothing else, even inside the VPC.
  assert {
    condition = alltrue([
      for rule in aws_security_group.rds[0].ingress :
      length(rule.cidr_blocks) == 0 && length(rule.security_groups) > 0
    ])
    error_message = "RDS ingress must be scoped to a security group, never a CIDR block"
  }

  assert {
    condition = alltrue([
      for rule in aws_security_group.redis[0].ingress :
      length(rule.cidr_blocks) == 0 && length(rule.security_groups) > 0
    ])
    error_message = "Redis ingress must be scoped to a security group, never a CIDR block"
  }
}
