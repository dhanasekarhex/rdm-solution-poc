from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets

from constructs import Construct
import os

DIRNAME = os.path.dirname(__file__)

class RdmSolutionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create lambda function
        s3_lambda_sync_fn = _lambda.Function(
            self,
            "s3_data_sync",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler='lambda_function.handler',
            code=_lambda.Code.from_asset(os.path.join(DIRNAME, "lambda")),
        )

        # Add policy to allow the lambda function to access s3
        s3_lambda_sync_fn.add_to_role_policy(
            statement=_iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                actions=['s3:*'],
                resources=['arn:aws:s3:::*']
            )
        )

        # add enviroment variable to lambda function
        s3_lambda_sync_fn.add_environment('ENV', "dev")

        # Set up an Amazon EventBridge rule to trigger the Lambda function 
        event_rule = events.Rule(
            self,
            "StartGlueJobRule",
            event_pattern=events.EventPattern(
                detail_type=["Glue Crawler State Change"],
                source=["aws.glue"],
                detail={
                    "state":["SUCCEEDED"],
                    "crawlerName": ["rdm_solution_crawler"]
                }
            )
        )

        event_rule.add_target(targets.LambdaFunction(s3_lambda_sync_fn))

