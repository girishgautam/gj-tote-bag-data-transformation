# Lambda permissions

data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
    name_prefix = "data-squid-lambda"
    assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}


# Lambda permissions to S3

data "aws_iam_policy_document" "read_write_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:Describe*",
      "s3:Get*",
      "s3:List*",
      "s3:Put*",
      "s3-object-lambda:Get*",
      "s3-object-lambda:List*",
      "s3-object-lambda:Put*"
    ]
    resources = [
        "*"
        # # ClientError: "errorMessage": "An error occurred (AccessDenied)
        # # when calling the ListBuckets operation: User:
        # # arn:aws:sts::195275662632:assumed-role/data-squid-lambda20250225122007303900000001/extract_lambda
        # # is not authorized to perform: s3:ListAllMyBuckets because no
        # # identity-based policy allows the s3:ListAllMyBuckets action"
        # #
        # "${aws_s3_bucket.ingest_bucket.arn}/*",
        # "${aws_s3_bucket.transform_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "read_write_s3" {
  name_prefix = "s3-policy-lambda-"
  policy      = data.aws_iam_policy_document.read_write_s3.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.read_write_s3.arn
}


# Lambda permissions to Secrets Manager

data "aws_iam_policy_document" "read_secretsmanager" {  
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [
      "arn:aws:secretsmanager:eu-west-2:195275662632:secret:totesys_database-RBM0fV"
    ]  
  }
}

resource "aws_iam_policy" "read_secretsmanager" {
  name_prefix = "data-squid-read-secretsmanager"
  policy = data.aws_iam_policy_document.read_secretsmanager.json
}

resource "aws_iam_role_policy_attachment" "read_secretsmanager" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.read_secretsmanager.arn
}


# Cloudwatch permissions

data "aws_iam_policy_document" "cloudwatch-policy" {
  statement{
    effect = "Allow"
    resources = [
      "*"
      #"arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    actions = [
      "*"
    ]

  
  #   principals{
  #       type        = "Service"
  #       identifiers = ["events.amazonaws.com", "lambda.amazonaws.com"]
  # }
  }
}

resource "aws_iam_policy" "cloudwatch-policy" {
  name_prefix = "cloudwatch-policy-"
  policy = data.aws_iam_policy_document.cloudwatch-policy.json
}

resource "aws_iam_role_policy_attachment" "extract-lambda-cloudwatch" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch-policy.arn
}


# event bridge permissions

resource "aws_lambda_permission" "extract_lambda" {
  statement_id = "AllowExecutionFromCloudwatch"
  action = "lambda:InvocationFunction"
  principal = "events.amazonaws.com"
  function_name = aws_lambda_function.extract_lambda.function_name
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
}


# create iam for sns

data "aws_iam_policy_document" "ingestion_sns_topic_policy" {

  statement {
    effect = "Allow"
    actions = [
      "SNS:Subscribe",
      "SNS:Receive",
      "SNS:Publish"
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"
      values = [
        195275662632
      ]
    }

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      aws_sns_topic.email_alert.arn
    ]
  }
}


resource "aws_lambda_permission" "allow_load_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.transform_bucket.arn
}
# s3 Permissions for ingest bucket to invoke transform lambda

resource "aws_lambda_permission" "allow_ingest_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingest_bucket.arn
}
