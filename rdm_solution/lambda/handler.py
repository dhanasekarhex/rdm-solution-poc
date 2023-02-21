import os

from aws_cdk import (
    aws_glue as glue,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_iam as iam,
)

def handler(self, event, context):
    env = os.environ['ENV']

    # Create IAM Glue role
    glue_role = iam.Role(
            self, "GlueRole",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com')
        )

    # Create a VPC for the RDS Instance
    # vpc = ec2.Vpc(self, "Vpc", max_azs=2)

    # Create an RDS PostgreSQL instance
    # rds_instance = rds.DatabaseInstance(self, "RDSInstance",
    #                                     engine=rds.DatabaseInstanceEngine.postgres(
    #                                     version=rds.PostgresEngineVersion.VER_12_7
    #                                     ),
    #                                     instance_type=ec2.InstanceType.of(
    #                                     ec2.InstanceClass.BURSTABLE2, 
    #                                     ec2.InstanceSize.SMALL),
    #                                     vpc=vpc,
    #                                     allocated_storage=20,
    #                                     master_username="admin",
    #                                     master_user_password="test@123",
    #                                     #removal_policy=core.RemovalPolicy.DESTROY,
    #                                     )
    
    # Create Glue Database
    glue_database = glue.CfnDatabase(self, "GlueDatabase",
                                     database_input={
                                        "name": "glue_database"
                                     })
    
    # Create a Glue Connection to the RDS Instance
    rds_connection = glue.CfnConnection(self, "RDSConnection",
                                        connection_input={
                                            "name": "glue_rds_connection",
                                            "connectionType": "JDBC",
                                            "connectionProperties": {
                                                "JDBC_CONNECTION_URL": f'jdbc:postgresql://rdm.c4lpxufomrfq.eu-west-2.rds.amazonaws.com:5432/postgres',
                                                "JDBC_ENFORCE_SSL": 'false',
                                                "USERNAME": "rdm_admin",
                                                "PASSWORD": "RDMadmin2023#"
                                            }
                                        })
    
    # Create a Glue crawler to discover tables in the RDS Instance
    rds_crawler = glue.CfnCrawler(self, "RDSCrawler", 
                                  role=glue_role,
                                  database_name=glue_database.ref,
                                  targets={
                                    "jdbcTargets": [{
                                            "connectionName": rds_connection.ref,
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

    return f'{env}'