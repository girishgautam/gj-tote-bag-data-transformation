# resource "aws_cloudwatch_event_rule" "scheduler" {
#   # This should set up a scheduler that will trigger the Lambda
#   # Careful! other things may need to be set up as well
#   name = "trigger-extraction-lambda"
#   schedule_expression = "rate(5 minutes)"
#   description = "trigger extraction lambda"
# }

# resource "aws_cloudwatch_event_target" "sns" {
#   rule      = aws_cloudwatch_event_rule.scheduler.name
#   target_id = "InvokeExtractionLambda"
#   # target id not required just a label to help identify target

#   arn       = aws_lambda_function.extract_lambda.arn
# }

# resource "aws_lambda_permission" "event_permissions" {
#   function_name = aws_lambda_function.extract_lambda.function_name
#   action = "lambda:InvokeFunction"
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.scheduler.arn
# }

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingest_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "reports/"
    filter_suffix       = "_success.json"
  }

  depends_on = [aws_lambda_permission.allow_ingest_bucket]
}

resource "aws_s3_bucket_notification" "load_bucket_notification" {
  bucket = aws_s3_bucket.transform_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "reports/"
    filter_suffix       = "_success.json"
  }

  depends_on = [aws_lambda_permission.allow_load_bucket]
}
