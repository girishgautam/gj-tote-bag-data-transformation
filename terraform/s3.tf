resource "aws_s3_bucket" "code_bucket"{
    bucket_prefix = "data-squid-code-"

}
resource "aws_s3_object" "extract_lambda_code"{
    bucket = aws_s3_bucket.code_bucket.bucket
    key = "extract_lambda/function.zip"
    source = "${path.module}/../packages/extract_lambda/function.zip"
    #etag = filemd5("${path.module}/../packages/extract_lambda/function.zip")
}

# resource "aws_s3_object" "lambda_code"{
#     bucket = aws_s3_bucket.code_bucket.bucket
#     key = "${each.key}/function.zip"
#     source = "${path.module}/../packages/${each.key}/function.zip"
#     for_each = toset([var.extract_lambda, var.transform_lambda, var.load_lamabda])
#     etag = filemd5("${path.module}/../packages/${each.key}/function.zip")
    
# }

resource "aws_s3_bucket" "ingest_bucket"{
    bucket_prefix = "data-squid-ingest-bucket-"

}
resource "aws_s3_bucket" "transform_bucket"{
    bucket_prefix = "data-squid-transform-bucket-"

}


resource "aws_s3_object" "extraction_utils" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = "${var.extraction_utils}/${var.extraction_utils_zip_filename}"
    source = "${path.module}/../packages/${var.extraction_utils}/extraction_utils.zip"
    # etag = filemd5("${path.module}/../packages/${var.extraction_utils}/extraction_utils.zip")
}



resource "aws_s3_object" "lambda_dependencies" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = "dependencies/${var.dependencies_zip_filename}"
    # source = "${path.module}/../packages/dependencies/dependencies.zip"
    source = "/home/runner/work/de-tote-bag-data-transformation/de-tote-bag-data-transformation/terraform/../packages/dependencies/dependencies.zip"
#   When expanding the plan for aws_s3_object.lambda_dependencies to include new values learned so far during apply, provider "registry.terraform.io/hashicorp/aws" produced an
#   invalid new value for .etag: was cty.StringVal("9c0a09b84ea412a1c5efe856c45ad8d1"), but now cty.StringVal("7e96ae7528f3817c03205d1f9407c564").
#  
#   This is a bug in the provider, which should be reported in the provider's own issue tracker.
    #etag = filemd5("${path.module}/../packages/dependencies/dependencies.zip")
    # source_hash = filemd5("${path.module}/../packages/dependencies/dependencies.zip")
    // filebase64sha256("/absolute/path/to/extract_layer.zip")
    # source_hash = filebase64sha256("${path.module}/../packages/dependencies/dependencies.zip")
}

# resource "aws_s3_object" "transform_lambda" {
#     bucket = aws_s3_bucket.code_bucket.bucket
#     key = "${var.transform_lambda}/${var.extraction_utils_zip_filename}"
#     source = data.archive_file.transform_lambda.output_path

# }