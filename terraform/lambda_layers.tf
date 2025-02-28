

resource "aws_lambda_layer_version" "extraction_utils_layer" {
  layer_name = "${var.extraction_utils}-layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = aws_s3_object.extraction_utils.key
}

resource "aws_lambda_layer_version" "dependencies_layer" {
  layer_name = "dependencies-layer"
  compatible_runtimes = ["python3.12"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = aws_s3_object.lambda_dependencies.key
}


#transform util layer
# resource "aws_lambda_layer_version" "transform_lambda_util_layer" {
#   layer_name = "${var.transform_lambda}-layer"
#   compatible_runtimes = ["python3.12"]
#   s3_bucket = aws_s3_bucket.code_bucket.bucket
#   s3_key = aws_s3_object.transform_lambda.key
# }
