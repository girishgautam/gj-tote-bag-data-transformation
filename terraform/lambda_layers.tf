resource "aws_lambda_layer_version" "utils_layer" {
  layer_name = "utils"
  compatible_runtimes = ["python3.12"]
  # s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_key = aws_s3_object.utils.key
  filename = "${path.module}/../packages/utils/utils.zip"
  source_code_hash = filebase64sha256("${path.module}/../packages/utils/utils.zip")
}

resource "aws_lambda_layer_version" "dependencies_layer" {
  layer_name = "dependencies-layer"
  compatible_runtimes = ["python3.12"]
  # s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_key = aws_s3_object.lambda_dependencies.key
  # Deployment method below, works if archive is < 70167211 bytes:
  filename = "${path.module}/../packages/dependencies/dependencies.zip"
  source_code_hash = filebase64sha256("${path.module}/../packages/dependencies/dependencies.zip")
}



