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
            self, "RDSConnection",
            catalog_id=self.account,
            connection_input={
                'name': 'PostgresqlConnection',
                'connectionType' : 'JDBC',
                'connectionProperties': {
                    'JDBC_CONNECTION_URL': f'jdbc:postgresql://rdm.c4lpxufomrfq.eu-west-2.rds.amazonaws.com:5432/postgres',
                    'JDBC_ENFORCE_SSL': 'false',
                    'USERNAME' : 'rdm_admin',
                    'PASSWORD': 'RDMadmin2023#'
                }
            }
        )

        # Create the Glue Database
        glue_database = glue.CfnDatabase(
            self, "glue_database",
            database_input={
                'name': 'rdm_glue_database',
                'description': 'My PostgreSQL Database'
            },
            catalog_id=glue_connection.catalog_id
        )

        # Create a Glue crawler to discover tables in the RDS Instance
        rds_crawler = glue.CfnCrawler(
            self, "RDSCrawler", 
            role=glue_role,
            database_name=glue_database.ref,
            targets={
                "jdbcTargets": [{
                        "connectionName": glue_connection.ref,
                        "path": f'/postgres/dev'
                    }]
            },
            schema_change_policy={
                "updateBehavior": "UPDATE_IN_DATABASE",
                "deleteBehavior": "LOG"
            },
            configuration={
                "Version": 1,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior": "InheritFromTable"
                    },
                    "Tables": {
                        "AddOrUpdateBehavior": "MergeNewColumns"
                    }
                }
            }
            )