# IRSA: bind an IAM role to the Kubernetes service account the gateway and
# reconciler run as.
#
# MSK Serverless supports *only* IAM authentication — there is no SASL password
# to put in a Secret. Pods get short-lived credentials from the cluster's OIDC
# provider instead, so no long-lived AWS key exists anywhere in the deployment.
#
# The application side of this is a SASL/OAUTHBEARER token provider; see
# `kafka_sasl_*` in the chart values and `inference_ledger.bus`.

data "aws_iam_policy_document" "msk_access" {
  count = local.managed ? 1 : 0

  # Connect to the cluster itself.
  statement {
    sid = "Connect"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
    ]
    resources = [aws_msk_serverless_cluster.this[0].arn]
  }

  # Produce, consume and create topics. Topic creation is included because the
  # chart's post-install Job bootstraps the five topics.
  statement {
    sid = "Topics"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
    ]
    resources = ["${replace(aws_msk_serverless_cluster.this[0].arn, ":cluster/", ":topic/")}/*"]
  }

  # Consumer-group membership for the reconciler.
  statement {
    sid = "Groups"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = ["${replace(aws_msk_serverless_cluster.this[0].arn, ":cluster/", ":group/")}/*"]
  }
}

resource "aws_iam_policy" "msk_access" {
  count = local.managed ? 1 : 0

  name        = "${var.name}-msk-access"
  description = "Produce/consume on the inference-ledger MSK cluster"
  policy      = data.aws_iam_policy_document.msk_access[0].json

  tags = local.tags
}

module "irsa" {
  count = local.managed ? 1 : 0

  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.48"

  role_name = "${var.name}-app"

  role_policy_arns = {
    msk = aws_iam_policy.msk_access[0].arn
  }

  oidc_providers = {
    main = {
      provider_arn = module.eks.oidc_provider_arn
      namespace_service_accounts = [
        "${local.service_account_namespace}:${local.service_account_name}",
      ]
    }
  }

  tags = local.tags
}
