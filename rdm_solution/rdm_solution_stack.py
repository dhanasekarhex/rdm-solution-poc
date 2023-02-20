from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam

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
