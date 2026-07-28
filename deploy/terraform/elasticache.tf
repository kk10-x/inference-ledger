# ElastiCache Redis: idempotency claims and per-tenant token buckets.
#
# Everything Redis holds here is deliberately ephemeral — in-flight idempotency
# leases expire on their own, and token buckets refill from elapsed time. Losing
# the cache costs a brief window of duplicate-request protection, not money,
# which is why a single node is acceptable where it would not be for the ledger.

resource "aws_security_group" "redis" {
  count = local.managed ? 1 : 0

  name        = "${var.name}-redis"
  description = "Redis access from the EKS node group"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Redis from cluster nodes"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  tags = merge(local.tags, { Name = "${var.name}-redis" })
}

resource "aws_elasticache_subnet_group" "this" {
  count = local.managed ? 1 : 0

  name       = var.name
  subnet_ids = module.vpc.private_subnets

  tags = local.tags
}

# The default parameter group evicts keys under memory pressure. That would
# silently reset tenants' token buckets and drop idempotency claims — handing out
# free tokens and re-admitting duplicate requests, both invisibly. noeviction
# turns memory pressure into a loud error instead of quiet incorrectness.
resource "aws_elasticache_parameter_group" "this" {
  count = local.managed ? 1 : 0

  name   = var.name
  family = "redis7"

  parameter {
    name  = "maxmemory-policy"
    value = "noeviction"
  }

  tags = local.tags
}

resource "aws_elasticache_cluster" "this" {
  count = local.managed ? 1 : 0

  cluster_id           = var.name
  engine               = "redis"
  engine_version       = var.elasticache.engine_version
  node_type            = var.elasticache.node_type
  num_cache_nodes      = var.elasticache.num_cache_nodes
  parameter_group_name = aws_elasticache_parameter_group.this[0].name
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.this[0].name
  security_group_ids = [aws_security_group.redis[0].id]

  tags = local.tags
}
