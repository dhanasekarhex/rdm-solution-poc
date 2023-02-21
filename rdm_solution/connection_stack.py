from aws_cdk import (
    Stack
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_glue as glue

from constructs import Construct
import os

DIRNAME = os.path.dirname(__file__)

class ConnectionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create IAM Glue role
        glue_role = _iam.Role(
                self, "GlueRole",
                assumed_by=_iam.ServicePrincipal('glue.amazonaws.com')
            )
        
        # Create the Glue Connection
        glue_connection = glue.CfnConnection(
            self, "GlueConnection",
            connection_input={
                'name': 'PostgresqlConnection',
                'connection_type' : 'JDBC',
                'connection_properties': {
                    'JDBC_CONNECTION_URL': f'jdbc:postgresql://rdm.c4lpxufomrfq.eu-west-2.rds.amazonaws.com:5432/postgres',
                    'JDBC_ENFORCE_SSL': 'false',
                    'USERNAME' : 'rdm_admin',
                    'PASSWORD': 'RDMadmin2023#'
                }
            }
        )

        # Create the Glue Database
        glue_database = glue.CfnDatabase(
            self, "GlueDatabase",
            database_input={
                'name': 'myGlueDatabase',
                'description': 'My PostgreSQL Database'
            },
            catalog_id="123456789012"
        )