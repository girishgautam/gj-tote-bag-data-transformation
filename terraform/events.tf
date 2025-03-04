resource "aws_cloudwatch_event_rule" "scheduler" {
  # This should set up a scheduler that will trigger the Lambda
  # Careful! other things may need to be set up as well
  name = "trigger-extraction-lambda"
  schedule_expression = "rate(15 minutes)"
  description = "trigger extraction lambda"
}

resource "aws_cloudwatch_event_target" "sns" {
  rule      = aws_cloudwatch_event_rule.scheduler.function_name
  target_id = "InvokeExtractionLambda"
  # target id not required just a label to help identify target

  arn       = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "event_permissions" {
  function_name = aws_lambda_function.extract_lambda.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
}