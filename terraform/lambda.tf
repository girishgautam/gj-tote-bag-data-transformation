# Archived code for extract lambda
data "archive_file" "extract_lambda"{
    type = "zip"
    output_file_mode = "0666"
    source_file = "${path.module}/../src/extraction_lambda/main.py"
    output_path = "${path.module}/../packages/${var.extract_lambda}/function.zip"
}

# Create extract lambda
resource "aws_lambda_function" "extract_lambda" {
    role = aws_iam_role.lambda_role.arn
    function_name = var.extract_lambda
    layers = [
      aws_lambda_layer_version.utils_layer.arn,
      aws_lambda_layer_version.dependencies_layer.arn
    ]
    filename = data.archive_file.extract_lambda.output_path
    source_code_hash = filebase64sha256(data.archive_file.extract_lambda.output_path)
    handler = "main.lambda_handler"
    timeout = 900
    runtime = "python3.12"

    environment {
    variables = {
      BUCKET_INGEST = aws_s3_bucket.ingest_bucket.bucket
    }
  }
}

# Archived code for transform lambda
data "archive_file" "transform_lambda"{
    type = "zip"
    output_file_mode = "0666"
    source_file = "${path.module}/../src/transform_lambda/main.py"
    output_path = "${path.module}/../packages/${var.transform_lambda}/function.zip"
}

# Create transform lambda
resource "aws_lambda_function" "transform_lambda" {
    role = aws_iam_role.lambda_role.arn
    function_name = var.transform_lambda
    layers = [
      aws_lambda_layer_version.utils_layer.arn,
      aws_lambda_layer_version.dependencies_layer.arn
    ]
    filename = data.archive_file.transform_lambda.output_path
    source_code_hash = filebase64sha256(data.archive_file.transform_lambda.output_path)
    handler = "main.lambda_handler"
    timeout = 900
    runtime = "python3.12"

    environment {
      variables = {
        BUCKET_TRANSFORM = aws_s3_bucket.transform_bucket.bucket
        BUCKET_INGEST = aws_s3_bucket.ingest_bucket.bucket
      }
    }
}


