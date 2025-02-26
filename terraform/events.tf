#resource "aws_cloudwatch_event_rule" "scheduler" {
  #TODO: this should set up a scheduler that will trigger the Lambda every 5 minutes
  # Careful! other things may need to be set up as well
#   name = "trigger-lambda-5-min"
#   schedule_expression = "rate( minutes)"
#   description = "trigger every  minutes"
#}

#resource "aws_cloudwatch_event_target" "sns" {
#   rule      = aws_cloudwatch_event_rule.scheduler.name
#   target_id = "InvokeLambda"
  #target id not required just a label to help identify target

#   arn       = aws_lambda_function.quote_handler.arn
#}