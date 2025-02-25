data "archive_file" "extract_lambda"{
    type = "zip"
    output_file_mode = "0664"
    source_file = "${path.module}/../src/extraction_lambda/main.py"
    output_path = "${path.module}/../packages/${var.extract_lambda}/function.zip"
}


# resource "aws_lambda_function" "extract_lambda" {
#     role = resource.aws_iam_role.lambda_role.arn
#     function_name = var.extract_lambda
#     s3_bucket = aws_s3_bucket.code_bucket.bucket

# }


# data "archive_file" "transform_lambda" {
#     type = "zip"
#     output_file_mode = "0664"
#     source_file = "${path.module}/.."
#     output_path = "${path.module}/../packages/transform_lambda.zip"
# }