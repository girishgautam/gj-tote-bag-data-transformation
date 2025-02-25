data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  
}
}
data "aws_iam_policy_document" "read_write_s3" {
  statement {
    actions = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]

    resources = []
  
}
}

resource "aws_iam_role" "lambda_role" {
    name_prefix = "data-squid-lambda"
    assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}
