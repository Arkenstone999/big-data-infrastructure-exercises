terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region  = "us-east-1"
}

resource "aws_instance" "app_server" {
  ami           = "ami-053a45fff0a704a47"
  instance_type = "t2.micro"

  iam_instance_profile = "LabInstanceProfile" 

  tags = {
    Name = "bdi_aircraft_charles_frelet"
  }
}
