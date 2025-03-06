#Archived code for extract lambda
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

# data "archive_file" "utils"{
#     type = "zip"
#     output_file_mode = "0666"
#     source_dir = "${path.module}/../utils"
#     output_path = "${path.module}/../packages/utils/utils.zip"
# }

# data "archive_file" "dependencies"{
#     type = "zip"
#     output_file_mode = "0666"
#     source_dir = "${path.module}/../dependencies/python/"
#     output_path = "${path.module}/../packages/dependencies/dependencies.zip"
# }




# todo Uncomment when team has added /transformation_lambda/main.py
#transform lambda archive
# data "archive_file" "transform_lambda" {
#     type = "zip"
#     output_file_mode = "0666"
#     source_file = "${path.module}/../transformation_lambda/main.py"
#     output_path = "${path.module}/../packages/transform_lambda.zip"
# }

#provision transform lambda
# resource "aws_lambda_function" "transform_lambda" {
#     role = aws_iam_role.lambda_role.arn
#     function_name = var.transform_lambda
#     s3_bucket = aws_s3_bucket.code_bucket.bucket
#     s3_key = aws_s3_object.transform_lambda.key
#     layers = [
#       # aws_lambda_layer_version.lambda_layer.arn,
#       aws_lambda_layer_version.transform_lambda_layer.arn,
#       aws_lambda_layer_version.dependencies_layer.arn
#     ]
#     handler = "${var.transform_lambda}.lambda_handler"
#     timeout = 900
#     runtime = "python3.12"

#     environment {
#     variables = {
#       BUCKET_TRANSFORM = aws_s3_bucket.transform_bucket.bucket
#     }
#   }
# }


data "archive_file" "load_lambda"{
    type = "zip"
    output_file_mode = "0666"
    source_file = "${path.module}/../src/load_lambda/main.py"
    output_path = "${path.module}/../packages/${var.load_lambda}/function.zip"
}

resource "aws_lambda_function" "load_lambda" {
    role = aws_iam_role.lambda_role.arn
    function_name = var.load_lambda
    layers = [
      aws_lambda_layer_version.utils_layer.arn,
      aws_lambda_layer_version.dependencies_layer.arn
    ]
    filename = data.archive_file.load_lambda.output_path
    source_code_hash = filebase64sha256(data.archive_file.load_lambda.output_path)
    handler = "main.lambda_handler"
    timeout = 900
    runtime = "python3.12"

    environment {
      variables = {
        BUCKET_TRANSFORM = aws_s3_bucket.transform_bucket.bucket
      }
  }
}
