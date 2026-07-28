terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State is local by default so `terraform validate` works with no cloud
  # account at all. For anything shared or long-lived, switch to an S3 backend
  # with DynamoDB locking — a local state file is a single point of failure and
  # cannot be locked against a concurrent apply.
  #
  # backend "s3" {
  #   bucket         = "your-tfstate-bucket"
  #   key            = "inference-ledger/terraform.tfstate"
  #   region         = "ap-south-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

# Authenticates by shelling out to `aws eks get-token` at apply time rather than
# baking a token into state. A token is valid for ~15 minutes, so an embedded one
# would break every apply after the first.
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}
