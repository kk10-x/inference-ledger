output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "configure_kubectl" {
  description = "Point kubectl at the new cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "deployment_profile" {
  description = "Which profile was applied."
  value       = var.deployment_profile
}

output "msk_bootstrap_brokers" {
  description = "MSK Serverless SASL/IAM bootstrap endpoint (full profile only)."
  value       = local.managed ? aws_msk_serverless_cluster.this[0].bootstrap_brokers_sasl_iam : null
}

output "rds_endpoint" {
  description = "RDS Postgres endpoint (full profile only)."
  value       = local.managed ? aws_db_instance.this[0].endpoint : null
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint (full profile only)."
  value       = local.managed ? aws_elasticache_cluster.this[0].cache_nodes[0].address : null
}

output "irsa_role_arn" {
  description = "IAM role the application service account assumes (full profile only)."
  value       = local.managed ? module.irsa[0].iam_role_arn : null
}

output "postgres_dsn" {
  description = "Ledger DSN. Sensitive: contains the generated database password."
  sensitive   = true
  value = local.managed ? format(
    "postgresql://%s:%s@%s/%s",
    aws_db_instance.this[0].username,
    random_password.db[0].result,
    aws_db_instance.this[0].endpoint,
    aws_db_instance.this[0].db_name,
  ) : null
}

# The whole point of the profile switch: this prints the exact `helm install`
# for what was actually provisioned, so wiring the chart to the infrastructure is
# copy-paste rather than a manual mapping exercise nobody gets right first time.
output "helm_install" {
  description = "Command to deploy the chart against this infrastructure."
  sensitive   = true
  value = local.managed ? trimspace(<<-EOT
    helm upgrade --install il deploy/helm/inference-ledger \
      --set redpanda.enabled=false \
      --set postgres.enabled=false \
      --set redis.enabled=false \
      --set-string endpoints.kafkaBootstrap='${aws_msk_serverless_cluster.this[0].bootstrap_brokers_sasl_iam}' \
      --set-string endpoints.postgresDsn='postgresql://${aws_db_instance.this[0].username}:${random_password.db[0].result}@${aws_db_instance.this[0].endpoint}/${aws_db_instance.this[0].db_name}' \
      --set-string endpoints.redisUrl='redis://${aws_elasticache_cluster.this[0].cache_nodes[0].address}:6379/0' \
      --set kafka.saslIamEnabled=true \
      --set-string kafka.awsRegion='${var.region}' \
      --set-string serviceAccount.roleArn='${module.irsa[0].iam_role_arn}' \
      --set-string provider.apiKey="$PROVIDER_API_KEY"
    EOT
    ) : trimspace(<<-EOT
    # "minimal" profile: the chart's bundled Redpanda/Postgres/Redis run in-cluster.
    helm upgrade --install il deploy/helm/inference-ledger \
      --set-string provider.apiKey="$PROVIDER_API_KEY"
    EOT
  )
}
