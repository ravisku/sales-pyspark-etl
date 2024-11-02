provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket  = "terraform-state-bucket-etl"
    key     = "terraform.tfstate"
    region  = "eu-west-2"
    encrypt = true
  }
}


resource "aws_iam_role" "glue_role" {
  name = "glue-etl-role"

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [{
      "Action" : "sts:AssumeRole",
      "Principal" : {
        "Service" : "glue.amazonaws.com"
      },
      "Effect" : "Allow",
      "Sid" : ""
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_s3_bucket" "sales-pyspark-etl" {
  bucket = "sales-pyspark-etl"
}

resource "aws_iam_policy" "glue_s3_access_policy" {
  name        = "glue-s3-access-policy"
  description = "Policy for S3 access to a specific bucket for Glue jobs"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        "Resource": [
          "arn:aws:s3:::sales-pyspark-etl",
          "arn:aws:s3:::sales-pyspark-etl/*"
        ]
      }
    ]
  })
}

# Attach the S3 access policy to the Glue role
resource "aws_iam_role_policy_attachment" "glue_s3_policy_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_access_policy.arn
}


# Define Glue ETL Job for Bronze Layer
resource "aws_glue_job" "bronze_job" {
  name     = "bronze-etl-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    # GitHub Actions will provide the location of the script in S3
    script_location = var.bronze_script_location
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files" = var.common_utils_location
  }
}

# Define Glue ETL Job for Silver Layer
resource "aws_glue_job" "silver_job" {
  name     = "silver-etl-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = var.silver_script_location
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files" = var.common_utils_location
  }
}


# Variables for script and dependency locations
variable "bronze_script_location" {}
variable "silver_script_location" {}
variable "common_utils_location" {}
