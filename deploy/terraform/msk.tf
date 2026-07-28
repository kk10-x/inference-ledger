# MSK Serverless: the managed replacement for the in-cluster Redpanda.
#
# Serverless has no broker count or instance type — you get IAM auth only, and
# you cap throughput. That IAM-only constraint is why irsa.tf exists: the app
# authenticates with a signed token from its pod identity, not a SASL password.

resource "aws_security_group" "msk" {
  count = local.managed ? 1 : 0

  # False positive: this group IS attached — to the MSK cluster via its
  # vpc_config below. The check only recognises attachment to EC2 instances and
  # ENIs, which a managed service does not expose.
  #checkov:skip=CKV2_AWS_5:attached to aws_msk_serverless_cluster.this via vpc_config

  name        = "${var.name}-msk"
  description = "MSK Serverless access from the EKS node group"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Kafka IAM (SASL_SSL) from cluster nodes"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name}-msk" })
}

resource "aws_msk_serverless_cluster" "this" {
  count = local.managed ? 1 : 0

  cluster_name = var.name

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.msk[0].id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = local.tags
}

# Topics are NOT created here — Terraform has no reachable admin API for a
# private MSK cluster, and the Helm chart already creates them from a
# post-install Job that runs inside the VPC. Keeping topic creation in one place
# means the partition count cannot drift between environments.
