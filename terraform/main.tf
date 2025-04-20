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
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}
variable "suffix" {
  type    = string
  default = "charles-frelet"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

data "aws_iam_instance_profile" "lab" {
  name = "LabInstanceProfile"
}

locals {
  subnets = data.aws_subnets.default.ids
}

#── SECURITY GROUP: API ──
resource "aws_security_group" "api_sg" {
  name        = "api-sg-${var.suffix}"
  description = "Allow SSH & FastAPI"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "FastAPI"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "api-sg-${var.suffix}"
  }
}

#── SECURITY GROUP: Airflow ──
resource "aws_security_group" "airflow_sg" {
  name        = "airflow-sg-${var.suffix}"
  description = "Allow SSH & Airflow UI"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "airflow-sg-${var.suffix}"
  }
}

#── SECURITY GROUP: RDS ──
resource "aws_security_group" "rds_sg" {
  name        = "rds-sg-${var.suffix}"
  description = "Allow PostgreSQL access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Postgres from API"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.api_sg.id]
  }

  ingress {
    description     = "Postgres from Airflow"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.airflow_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "rds-sg-${var.suffix}"
  }
}

#── SINGLE S3 BUCKET ──
resource "aws_s3_bucket" "aircraft_pipeline" {
  bucket        = "bdi-aircraft-charles"
  force_destroy = true

  tags = {
    Name = "aircraft-pipeline-${var.suffix}"
  }
}

#── RDS SUBNET GROUP ──
resource "aws_db_subnet_group" "db_subnet_group" {
  name        = "aircraft-db-subnet-group-${var.suffix}"
  description = "DB subnet group"
  subnet_ids  = local.subnets

  tags = {
    Name = "bdi_db_subnet_group_${var.suffix}"
  }

  lifecycle {
    create_before_destroy = true
  }
}

#── RDS POSTGRESQL INSTANCE ──
resource "aws_db_instance" "aircraft_db" {
  identifier           = "aircraft-db-${var.suffix}"
  engine               = "postgres"
  engine_version       = "13.20"
  instance_class       = "db.t3.micro"
  allocated_storage    = 20
  storage_type         = "gp2"
  db_name              = "aircraft"
  username             = "postgres"
  password             = "Martitheboss"
  db_subnet_group_name = aws_db_subnet_group.db_subnet_group.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  skip_final_snapshot  = true
  publicly_accessible  = true

  tags = {
    Name = "bdi_rds_${var.suffix}"
  }
}

#── EC2: FASTAPI SERVER ──
resource "aws_instance" "api_server" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t2.micro"
  iam_instance_profile        = data.aws_iam_instance_profile.lab.name
  subnet_id                   = local.subnets[0]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.api_sg.id]

  tags = {
    Name = "bdi_api_${var.suffix}"
  }
}

#── EC2: AIRFLOW SERVER ──
resource "aws_instance" "airflow_server" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t2.micro"
  iam_instance_profile        = data.aws_iam_instance_profile.lab.name
  subnet_id                   = local.subnets[1]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.airflow_sg.id]

  tags = {
    Name = "bdi_airflow_${var.suffix}"
  }
}

#── OUTPUTS ──
output "aircraft_s3_bucket" {
  description = "S3 bucket for both raw & prepared data"
  value       = aws_s3_bucket.aircraft_pipeline.bucket
}

output "rds_endpoint" {
  description = "PostgreSQL endpoint"
  value       = aws_db_instance.aircraft_db.endpoint
}

output "api_public_ip" {
  description = "FastAPI EC2 public IP"
  value       = aws_instance.api_server.public_ip
}

output "airflow_public_ip" {
  description = "Airflow EC2 public IP"
  value       = aws_instance.airflow_server.public_ip
}
