# RDS Postgres: the settlement ledger.
#
# This is the system of record for what tenants get billed, so it is the one
# component where durability outranks cost — hence backups on, deletion
# protection configurable, and storage encrypted.

resource "aws_security_group" "rds" {
  count = local.managed ? 1 : 0

  name        = "${var.name}-rds"
  description = "Postgres access from the EKS node group"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Postgres from cluster nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  tags = merge(local.tags, { Name = "${var.name}-rds" })
}

resource "aws_db_subnet_group" "this" {
  count = local.managed ? 1 : 0

  name       = var.name
  subnet_ids = module.vpc.private_subnets

  tags = local.tags
}

# Generated rather than variable-supplied so a password never lands in a tfvars
# file or shell history. It still ends up in state — which is the standard
# argument for an encrypted remote backend (see versions.tf).
resource "random_password" "db" {
  count = local.managed ? 1 : 0

  length  = 32
  special = false # avoids URL-encoding hazards in the DSN
}

resource "aws_db_instance" "this" {
  count = local.managed ? 1 : 0

  identifier     = var.name
  engine         = "postgres"
  engine_version = var.rds.engine_version
  instance_class = var.rds.instance_class

  allocated_storage     = var.rds.allocated_storage
  max_allocated_storage = var.rds.allocated_storage * 4 # storage autoscaling
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "ledger"
  username = "ledger"
  password = random_password.db[0].result

  db_subnet_group_name   = aws_db_subnet_group.this[0].name
  vpc_security_group_ids = [aws_security_group.rds[0].id]
  publicly_accessible    = false
  multi_az               = var.rds.multi_az

  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"

  # Demo-friendly defaults. For production: deletion_protection = true and
  # skip_final_snapshot = false, so a stray `terraform destroy` cannot vaporise
  # the billing ledger.
  deletion_protection = false
  skip_final_snapshot = true

  performance_insights_enabled = false # chargeable beyond the 7-day free tier

  tags = local.tags
}

# The application creates its own tables on startup (PostgresLedger._apply_schema)
# rather than relying on an init-script mount, which is precisely what makes it
# work against a managed database like this one.
