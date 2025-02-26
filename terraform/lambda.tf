# Archived code for extract lambda
data "archive_file" "extract_lambda"{
    type = "zip"
    output_file_mode = "0664"
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
      aws_lambda_layer_version.lambda_layer.arn
    ]
    handler = "${var.extract_lambda}.lambda_handler"
    timeout = 60
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
#     output_file_mode = "0664"
#     source_file = "${path.module}/.."
#     output_path = "${path.module}/../packages/transform_lambda.zip"
# }