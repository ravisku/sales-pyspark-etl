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