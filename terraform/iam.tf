# Lambda permissions

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

    resources = [
        aws_s3_bucket.ingest_bucket.arn,
        aws_s3_bucket.transform_bucket.arn
    ]
  
}
}


resource "aws_iam_role" "lambda_role" {
    name_prefix = "data-squid-lambda"
    assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}


resource "aws_iam_policy" "read_write_s3" {
  name_prefix = "s3-policy-lambda-"
  policy      = data.aws_iam_policy_document.read_write_s3.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.read_write_s3.arn
}


# Cloudwatch permissions

data "aws_iam_policy_document" "cloudwatch-policy" {
  statement {
    effect = "Allow"
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    actions = ["Logs:CreatesLogGroup"]
  }
  statement {
    effect = "Allow"
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    actions = [
      "Logs:CreateLogStream",
      "Logs:PutLogEvents"
    ]
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

resource "aws_lambda_permission" "extract_lambda" {
  statement_id = "AllowExecutionFromCloudwatch"
  action = "lambda:InvocationFunction"
  principal = "events.amazonaws.com"
  function_name = aws_lambda_function.extract_lambda.function_name
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
}



#create iam for sns

data "aws_iam_policy_document" "ingestion_sns_topic_policy" {

  statement {
    actions = [
      "SNS:Subscribe",
      "SNS:Receive"
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"
      values = [
        195275662632
      ]
    }

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      aws_sns_topic.email_alert.arn
    ]
  }
}