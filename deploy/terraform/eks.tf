module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.name
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access       = var.cluster_public_access
  cluster_endpoint_public_access_cidrs = var.cluster_public_access_cidrs
  cluster_endpoint_private_access      = true

  # IRSA lets pods assume IAM roles via a service account, so no AWS key ever
  # has to be mounted into a container or baked into an image.
  enable_irsa = true

  # The creating principal gets cluster-admin, otherwise the operator who runs
  # `terraform apply` cannot then run `kubectl`.
  enable_cluster_creator_admin_permissions = true
  authentication_mode                      = "API_AND_CONFIG_MAP"

  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
    # The gateway needs no persistent volumes, but the bundled Postgres and
    # Redpanda do when running under the "minimal" profile.
    aws-ebs-csi-driver = { most_recent = true }
    # metrics-server is NOT an EKS addon; it is installed separately below —
    # without it the chart's HPA reports <unknown> and never scales.
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_group_size.min
      max_size       = var.node_group_size.max
      desired_size   = var.node_group_size.desired

      # Bump from the default 35 GB: image layers plus the bundled Redpanda's
      # emptyDir data fill a small root volume quickly.
      disk_size = 50

      labels = {
        workload = "inference-ledger"
      }
    }
  }

  tags = local.tags
}

# The chart's HorizontalPodAutoscaler scales on CPU utilisation, which requires
# a metrics API. EKS does not ship one, so the manifest is applied here rather
# than left as a manual step that silently makes autoscaling a no-op.
resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  version    = "3.12.2"
  namespace  = "kube-system"

  set {
    name  = "args[0]"
    value = "--kubelet-insecure-tls"
  }

  depends_on = [module.eks]
}
