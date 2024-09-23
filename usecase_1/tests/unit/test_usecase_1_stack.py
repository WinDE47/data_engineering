import aws_cdk as core
import aws_cdk.assertions as assertions

from usecase_1.usecase_1_stack import Usecase1Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in usecase_1/usecase_1_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = Usecase1Stack(app, "usecase-1")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
