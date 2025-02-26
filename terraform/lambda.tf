#todo install dependencies to packages using terraform

# resource "terraform_data" "create_dependencies" {
#     provisioner "local-exec" {
#         command = "pip install -r ../requirements.txt -t ../dependencies/python"
#         #interpreter = ["/bin/bash", "-c"]
#         #command = "echo 'hello world' > hello_world.txt"
#     }
#     # triggers = {
#     #     #dependencies = filemd5("${path.module}/../requirements.txt")
#     #     always_run = timestamp()
#     # }
# }

resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1("${path.module}/../requirements-lambda.txt")
  }
  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = "${path.module}/../bin/archive_dependencies.sh"
  }
}


# Archived code for extract lambda
data "archive_file" "extract_lambda"{
    type = "zip"
    output_file_mode = "0666"
    source_file = "${path.module}/../src/extraction_lambda/main.py"
    output_path = "${path.module}/../packages/${var.extract_lambda}/function.zip"
}

# Create extract lambda
resource "aws_lambda_function" "extract_lambda" {
    role = resource.aws_iam_role.lambda_role.arn
    function_name = var.extract_lambda
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "${var.extract_lambda}/function.zip"
    layers = [
      aws_lambda_layer_version.lambda_layer.arn,
      aws_lambda_layer_version.extraction_utils_layer.arn,
      aws_lambda_layer_version.dependencies_layer.arn
    ]
    handler = "${var.extract_lambda}.lambda_handler"
    timeout = 900
    runtime = "python3.12"

    environment {
    variables = {
      BUCKET_INGEST = aws_s3_bucket.ingest_bucket.bucket
    }
  }
}

# Lambda layer containing extract code
resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name = "lambda-layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "${var.extract_lambda}/function.zip"
}


# data "archive_file" "transform_lambda" {
#     type = "zip"
#     output_file_mode = "0666"
#     source_file = "${path.module}/.."
#     output_path = "${path.module}/../packages/transform_lambda.zip"
# }




data "archive_file" "extraction_utils"{
    type = "zip"
    output_file_mode = "0666"
    source_dir = "${path.module}/../utils/extraction_utils/"
    output_path = "${path.module}/../packages/extraction_utils/extraction_utils.zip"
}




resource "aws_lambda_layer_version" "extraction_utils_layer" {
  layer_name = "${var.extraction_utils}-layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "extraction_utils/extraction_utils.zip"
}


data "archive_file" "dependencies"{
    type = "zip"
    output_file_mode = "0666"
    source_dir = "${path.module}/../dependencies/"
    output_path = "${path.module}/../packages/dependencies/dependencies.zip"
}




resource "aws_lambda_layer_version" "dependencies_layer" {
  layer_name = "dependencies-layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_key = "dependencies/${var.dependencies_zip_filename}"
  s3_key = "dependencies/dependencies-2.zip"
}

