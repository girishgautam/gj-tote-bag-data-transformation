resource "aws_s3_bucket" "code_bucket"{
    bucket_prefix = "data-squid-code-"

}



resource "aws_s3_bucket" "ingest_bucket"{
    bucket_prefix = "data-squid-ingest-bucket-"

}
resource "aws_s3_bucket" "transform_bucket"{
    bucket_prefix = "data-squid-transform-bucket-"

}


# resource "aws_s3_object" "utils" {
#     bucket = aws_s3_bucket.code_bucket.bucket
#     key = "${var.utils}/${var.utils_zip_filename}"
#     source = "${path.module}/../packages/utils/utils.zip"
# }



# resource "aws_s3_object" "lambda_dependencies" {
#     bucket = aws_s3_bucket.code_bucket.bucket
#     key = "dependencies/${var.dependencies_zip_filename}"
#     source = data.archive_file.dependencies.output_path
# }
# resource "aws_s3_object" "transform_lambda" {
#     bucket = aws_s3_bucket.code_bucket.bucket
#     key = "${var.transform_lambda}/${var.extraction_utils_zip_filename}"
#     source = data.archive_file.transform_lambda.output_path

# }