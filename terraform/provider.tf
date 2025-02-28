terraform{
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 5.0"
        }
    }
    backend "s3" {
        bucket = "data-squid-tf-bucket"
        key = "de-project-specification/build.tfstate"
        region = "eu-west-2"
    }
}



provider "aws"{
    region = "eu-west-2"
    default_tags {
        tags = {
            project = "totesys"
            team = "data-squid"
            deployed_from = "terraform"
            repository = "de-project-specification"
        }

    }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


