terraform{
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 5.0"
        }
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







