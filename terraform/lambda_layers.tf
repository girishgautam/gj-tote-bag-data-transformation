

resource "aws_lambda_layer_version" "extraction_utils_layer" {
  layer_name = "utils"
  compatible_runtimes = ["python3.12"]
  # s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_key = aws_s3_object.extraction_utils.key
  filename = "${path.module}/../packages/extraction_utils/utils.zip"
  source_code_hash = filebase64sha256("${path.module}/../packages/extraction_utils/utils.zip")
}

resource "aws_lambda_layer_version" "dependencies_layer" {
  layer_name = "dependencies-layer"
  compatible_runtimes = ["python3.12"]
  # s3_bucket = aws_s3_bucket.code_bucket.bucket
  # s3_key = aws_s3_object.lambda_dependencies.key
  filename = "${path.module}/../packages/dependencies/dependencies.zip"
  source_code_hash = filebase64sha256("${path.module}/../packages/dependencies/dependencies.zip")
}


#transform util layer
# resource "aws_lambda_layer_version" "transform_lambda_util_layer" {
#   layer_name = "${var.transform_lambda}-layer"
#   compatible_runtimes = ["python3.12"]
#   s3_bucket = aws_s3_bucket.code_bucket.bucket
#   s3_key = aws_s3_object.transform_lambda.key
# }
