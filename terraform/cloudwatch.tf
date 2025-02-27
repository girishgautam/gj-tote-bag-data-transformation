resource "aws_cloudwatch_metric_alarm" "cw_alarm"{
    alarm_name = "ingestion_failure_alarm"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    metric_name = "Errors"
    namespace = "AWS/Lambda"
    period = 900
    statistic = "Sum"
    alarm_actions = [aws_sns_topic.email_alert.arn]
    alarm_description = "email alert for errors detected in cloudwatch"

}