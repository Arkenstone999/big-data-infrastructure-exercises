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
  region = "us-east-1"
}

# EC2 Instance
resource "aws_instance" "app_server" {
  ami                  = "ami-053a45fff0a704a47"
  instance_type        = "t2.micro"
  iam_instance_profile = "LabInstanceProfile"
  security_groups      = ["launch-wizard-6"]

  tags = {
    Name = "bdi_aircraft_charles_frelet"
  }
}

# Get Default VPC
data "aws_vpc" "default" {
  default = true
}

# Get Availability Zones
data "aws_availability_zones" "available" {
  state = "available"
}

# Get subnets in the default VPC
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Create DB Subnet Group with subnets from at least 2 AZs
resource "aws_db_subnet_group" "aircraft_db_subnet_group" {
  name        = "aircraft-db-subnet-group"
  description = "Subnet group for aircraft DB"
  subnet_ids  = slice(data.aws_subnets.default.ids, 0, 2) # Get first 2 subnets

  tags = {
    Name = "bdi_db_subnet_group_charles_frelet"
  }
}

# RDS Instance 
resource "aws_db_instance" "aircraft_db" {
  identifier           = "aircraft-db"
  allocated_storage    = 20
  storage_type         = "gp2"
  engine               = "postgres"
  engine_version       = "13.20"
  instance_class       = "db.t3.micro"
  db_name              = "aircraft"
  username             = "postgres"
  password             = "Martitheboss"
  parameter_group_name = "default.postgres13"
  db_subnet_group_name = aws_db_subnet_group.aircraft_db_subnet_group.name
  vpc_security_group_ids = ["sg-063ca45bf7abc7183"]
  skip_final_snapshot  = true
  
  tags = {
    Name = "bdi_rds_charles_frelet"
  }
}

# Output the connection information
output "ec2_public_ip" {
  value = aws_instance.app_server.public_ip
}

output "rds_endpoint" {
  value = aws_db_instance.aircraft_db.endpoint
}
