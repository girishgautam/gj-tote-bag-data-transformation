resource "aws_sns_topic" "email_alert" {
  name = "my-topic-with-policy"
  fifo_topic = false
}

resource "aws_sns_topic_subscription" "email_to_alert" {
    endpoint = "data-squid-alerts@yopmail.com"
    topic_arn = resource.aws_sns_topic.email_alert.arn
    protocol = "email"
}

resource "aws_sns_topic_policy" "email_alert" {
  arn = aws_sns_topic.email_alert.arn
  policy = data.aws_iam_policy_document.ingestion_sns_topic_policy.json
}



# resource "aws_sns_topic" "user_updates" {
#   name            = "user-updates-topic"
#   delivery_policy = <<EOF
# {
#   'http': {
#     'defaultHealthyRetryPolicy': {
#       'minDelayTarget': 20,
#       'maxDelayTarget': 20,
#       'numRetries': 3,
#       'numMaxDelayRetries': 0,
#       'numNoDelayRetries': 0,
#       'numMinDelayRetries': 0,
#       'backoffFunction': 'linear'
#     },
#     'disableSubscriptionOverrides': false,
#     'defaultThrottlePolicy': {
#       'maxReceivesPerSecond': 1
#     }
#   }
# }
# EOF
# }