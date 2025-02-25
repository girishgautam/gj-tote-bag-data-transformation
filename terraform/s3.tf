resource "aws_s3_bucket" "code_bucket"{
    bucket_prefix = "data-squid-code-"
}
resource "aws_s3_object" "lambda_code"{
    bucket = aws_s3_bucket.code_bucket.bucket
    key = "${each.key}/function.zip"
    source = "${path.module}/../packages/${each.key}/function.zip"
    for_each = toset([var.extract_lambda])#, var.transform_lambda, var.load_lamabda])
    etag = filemd5("${path.module}/../packages/${each.key}/function.zip")

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

resource "aws_s3_bucket" "load_bucket"{
    bucket_prefix = "data-squid-load-bucket-"

}