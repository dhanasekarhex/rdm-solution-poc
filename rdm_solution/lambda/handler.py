import os

from aws_cdk import (
    aws_glue as glue,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_iam as iam,
)

def handler(self, event, context):
    env = os.environ['ENV']
    return f'{env}'