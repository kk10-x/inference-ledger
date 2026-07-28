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

resource "aws_iam_role" "rds_monitoring" {
  count = local.managed && var.rds.enhanced_monitoring ? 1 : 0

  name = "${var.name}-rds-monitoring"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "monitoring.rds.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  count = local.managed && var.rds.enhanced_monitoring ? 1 : 0

  role       = aws_iam_role.rds_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

resource "aws_db_instance" "this" {
  count = local.managed ? 1 : 0

  # Multi-AZ, deletion protection, Performance Insights and enhanced monitoring
  # are all variable-driven and off by default for a low-cost, easily-destroyed
  # demo. They are deliberate cost trade-offs, not oversights — production flips
  # them via the rds variable (see terraform.tfvars.example).
  #checkov:skip=CKV_AWS_157:multi_az is var-driven; production sets it true
  #checkov:skip=CKV_AWS_293:deletion_protection is var-driven; production sets it true
  #checkov:skip=CKV_AWS_353:performance_insights is var-driven and chargeable
  #checkov:skip=CKV_AWS_118:enhanced_monitoring is var-driven and chargeable

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

  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"
  copy_tags_to_snapshot   = true

  # Security patches land without an operator remembering to apply them. Major
  # versions still require a deliberate upgrade.
  auto_minor_version_upgrade = true

  # Lets pods authenticate with their IRSA identity instead of the generated
  # password — the same posture MSK forces, available here as an option.
  iam_database_authentication_enabled = true

  # Postgres logs to CloudWatch. For a billing ledger, "who changed what" is
  # worth retaining independently of the database itself.
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  deletion_protection = var.rds.deletion_protection
  # A final snapshot is skipped only while deletion protection is off, i.e. in
  # the throwaway demo posture. Turning protection on also preserves the data.
  skip_final_snapshot       = !var.rds.deletion_protection
  final_snapshot_identifier = var.rds.deletion_protection ? "${var.name}-final" : null

  multi_az = var.rds.multi_az

  performance_insights_enabled = var.rds.performance_insights
  monitoring_interval          = var.rds.enhanced_monitoring ? 60 : 0
  monitoring_role_arn          = var.rds.enhanced_monitoring ? aws_iam_role.rds_monitoring[0].arn : null

  tags = local.tags
}

# The application creates its own tables on startup (PostgresLedger._apply_schema)
# rather than relying on an init-script mount, which is precisely what makes it
# work against a managed database like this one.
