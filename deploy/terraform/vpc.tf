module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.16"

  name = var.name
  cidr = var.vpc_cidr
  azs  = local.azs

  # /20 private (4094 usable) because pods draw VPC IPs through the AWS CNI —
  # a /24 per AZ runs out of addresses long before the nodes run out of CPU.
  private_subnets = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 8, i + 200)]

  enable_nat_gateway = true
  single_nat_gateway = var.single_nat_gateway
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Nodes, MSK, RDS and ElastiCache all live in private subnets. Public subnets
  # exist only for the NAT gateways and any future ingress load balancer.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }

  tags = local.tags
}

# S3 traffic (ECR image layers) leaves via this endpoint rather than the NAT
# gateway. Gateway endpoints are free and image pulls are the bulk of a cluster's
# egress, so this measurably cuts the NAT data-processing bill.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = merge(local.tags, { Name = "${var.name}-s3" })
}
