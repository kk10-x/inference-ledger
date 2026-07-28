# Does the profile switch actually control spend?
#
# The whole cost argument in README.md rests on one claim: "minimal" provisions
# no managed services. That is worth asserting rather than trusting, because the
# failure mode is silent and expensive — a stray resource that ignores the flag
# bills ~$0.75/hr for an MSK cluster nobody meant to create.
#
# Providers are mocked and the community VPC/EKS modules are stubbed, so these
# run with no AWS account, no credentials and no network. The point is to test
# *this* configuration's logic, not to re-test upstream modules.

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

run "minimal_provisions_no_managed_services" {
  command = plan

  variables {
    deployment_profile = "minimal"
  }

  assert {
    condition     = length(aws_msk_serverless_cluster.this) == 0
    error_message = "minimal profile created an MSK cluster — that is ~$0.75/hr nobody asked for"
  }

  assert {
    condition     = length(aws_db_instance.this) == 0
    error_message = "minimal profile created an RDS instance; Postgres should run in-cluster"
  }

  assert {
    condition     = length(aws_elasticache_cluster.this) == 0
    error_message = "minimal profile created ElastiCache; Redis should run in-cluster"
  }

  # No managed services means nothing to grant access to, so the IRSA role and
  # its security groups must not exist either.
  assert {
    condition     = length(aws_security_group.msk) == 0 && length(aws_security_group.rds) == 0
    error_message = "minimal profile created security groups for services it does not provision"
  }
}

run "full_provisions_exactly_one_of_each" {
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
    condition     = length(aws_msk_serverless_cluster.this) == 1
    error_message = "full profile must provision MSK Serverless"
  }

  assert {
    condition     = length(aws_db_instance.this) == 1
    error_message = "full profile must provision RDS"
  }

  assert {
    condition     = length(aws_elasticache_cluster.this) == 1
    error_message = "full profile must provision ElastiCache"
  }
}

run "rejects_an_unknown_profile" {
  command = plan

  variables {
    deployment_profile = "cheap-ish"
  }

  # A typo must fail the plan rather than silently falling through to a default,
  # since "not full" quietly means "no managed services".
  expect_failures = [var.deployment_profile]
}

run "rejects_a_single_az" {
  command = plan

  variables {
    az_count = 1
  }

  # MSK Serverless requires multi-AZ, and one AZ leaves no headroom during a
  # node-group rollout.
  expect_failures = [var.az_count]
}
