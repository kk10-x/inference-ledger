# Terraform — AWS deployment

Provisions the infrastructure the Helm chart's managed-service mode expects:
EKS, and optionally MSK Serverless, RDS Postgres and ElastiCache Redis.

> **Status: written and validated, never applied.** `terraform fmt`, `validate`
> and `plan` have been run; `apply` has not. Nothing in this directory has been
> billed for or observed running. The Kubernetes deployment *has* been verified
> — on a local [kind](../kind) cluster, using the same chart.

## The profile switch

`deployment_profile` decides how much AWS runs for you:

| | `minimal` | `full` |
|---|---|---|
| EKS + VPC + node group | ✅ | ✅ |
| Kafka | Redpanda pod | **MSK Serverless** |
| Postgres | Postgres pod | **RDS** |
| Redis | Redis pod | **ElastiCache** |
| Approx. cost | **~$0.19/hr** | **~$1.00/hr** |

Both are genuinely functional. `minimal` is not a toy — it is the same gateway
and reconciler, with the stateful parts self-hosted, which is exactly what the
Helm chart's bundled-infra toggle exists for. `full` is the production shape.

The split matters because **MSK Serverless alone is ~$0.75/hr** and dominates
the bill; a demo that only needs to prove the system runs on real cloud
Kubernetes does not need it.

### Approximate hourly cost (ap-south-1, on-demand)

| Component | `minimal` | `full` |
|---|---:|---:|
| EKS control plane | $0.10 | $0.10 |
| 2 × t3.medium nodes | $0.04 | $0.04 |
| NAT gateway (single) | $0.045 | $0.045 |
| MSK Serverless (base) | — | $0.75 |
| RDS db.t4g.micro | — | $0.016 |
| ElastiCache cache.t4g.micro | — | $0.017 |
| **Total** | **~$0.19/hr** | **~$0.97/hr** |

Excludes data transfer and storage, which are pennies at demo scale. The real
risk is not the hourly rate — it is a NAT gateway or EBS volume left running for
a week. **Destroy when done.**

## Use it

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # edit
terraform init
terraform plan          # free, read-only, creates nothing
terraform apply         # starts billing
```

Then wire the chart to what was built — the output prints the exact command:

```bash
aws eks update-kubeconfig --region <region> --name inference-ledger
terraform output -raw helm_install
```

Tear down completely:

```bash
terraform destroy
```

## Notes worth reading before applying

- **MSK Serverless is IAM-only.** There is no SASL password. Pods authenticate
  with short-lived tokens from an IRSA role (`irsa.tf`), and the app signs them
  via `inference_ledger.kafka_auth`. This is why `serviceAccount.roleArn` and
  `kafka.saslIamEnabled` exist in the chart values.
- **Topics are created by the chart, not Terraform.** Terraform cannot reach a
  private MSK cluster's admin API, and the chart already bootstraps the five
  topics from an in-VPC Job. One source of truth for partition counts.
- **Schema is applied by the app.** `PostgresLedger._apply_schema` runs on
  startup, which is what makes RDS work — there is no init-script mount on a
  managed database.
- **State is local.** Fine for a single operator; switch to the S3 backend
  stubbed in `versions.tf` for anything shared. Note the generated RDS password
  lands in state, so that state file is a secret.
- **`deletion_protection = false`** and `skip_final_snapshot = true` on RDS keep
  teardown frictionless for a demo. Both should be flipped for anything holding
  real billing data.
