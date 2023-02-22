from aws_cdk import (
    Stack
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_glue as glue
from aws_cdk import aws_ec2 as ec2

from constructs import Construct
import os

DIRNAME = os.path.dirname(__file__)

class GlueConnectionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create IAM Glue role
        glue_role = _iam.Role(
                self, "GlueRole",
                assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
                description="Role for Glue Crawler",
                managed_policies=[
                    _iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AWSGlueServiceRole"
                    ),
                    _iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonS3FullAccess"
                    ),
                    _iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonRDSFullAccess"
                    ),
                ]
            )
        
        # Add the required permissions to the role
        # glue_role.add_to_policy(
        #     _iam.PolicyStatement(
        #         effect=_iam.Effect.ALLOW,
        #         actions=['glue:*',],
        #         resources=["*"],
        #     )
        # )

        # Create VPC and Security groups
        vpc = ec2.Vpc(
            self, "rds_rdm_vpc", 
            max_azs=2
        )
        # vpc = ec2.CfnVPC(
        #     self,
        #     "rdm_vpc"
        # )

        security_group = ec2.SecurityGroup(
            self, 
            "rds_rdm_security_group", 
            vpc=vpc,
            allow_all_outbound=True,
            security_group_name="rds_rdm_security_group"
        )
        security_group.add_ingress_rule(
            ec2.Peer.ipv4('0.0.0.0/0'),
            ec2.Port.tcp(5432)
        )
        security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.all_traffic()
        )

        # security_group = ec2.CfnSecurityGroup(
        #     self,
        #     "rdm_solution_security_group",
        #     group_description="RDM Solution ETL",
        #     vpc_id=vpc.ref,
        #     security_group_egress=[
        #         {
        #             "cidrIp": "0.0.0.0/0",
        #             "ipProtocol": "-1",
        #         },
        #         {
        #             "cidrIp": "0.0.0.0/0",
        #             "ipProtocol": "tcp",
        #             "fromPort": 5432,
        #             "toPort": 5432
        #         }
        #     ]
        # )

        # Create the Glue Connection
        glue_connection = glue.CfnConnection(
            self, "rdm_connection",
            catalog_id=self.account,
            connection_input={
                'name': 'PostgresqlRDMConnection',
                'connectionType' : 'JDBC',
                'connectionProperties': {
                    'JDBC_CONNECTION_URL': f'jdbc:postgresql://rdm.c4lpxufomrfq.eu-west-2.rds.amazonaws.com:5432/postgres',
                    'JDBC_ENFORCE_SSL': 'false',
                    'USERNAME' : 'rdm_admin',
                    'PASSWORD': 'RDMadmin2023#'
                },
                "PhysicalConnectionRequirements" : {
                    "AvailabilityZone": "eu-west-2",
                    "SecurityGroupIdList": [
                        security_group.security_group_id
                    ],
                    "SubnetId": vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC).subnet_ids[0] ,
                },
            },
        )

        # Create the Glue Database
        glue_database = glue.CfnDatabase(
            self, "rdm_glue_database",
            database_input={
                'name': 'rdm_solution_glue_database',
                'description': 'My PostgreSQL Database'
            },
            catalog_id=glue_connection.catalog_id
        )
        

        # Create a Glue crawler to discover tables in the RDS Instance
        rds_crawler = glue.CfnCrawler(
            self, "rds_crawler", 
            role=glue_role.role_arn,
            name="rdm_solution_crawler",
            database_name=glue_database.ref,
            targets={
                "jdbcTargets": [{
                        "connectionName": glue_connection.ref,
                        "path": f'postgres/dev/%'
                    }]
            },
            schema_change_policy={
                "updateBehavior": "UPDATE_IN_DATABASE",
                "deleteBehavior": "LOG"
            },
            configuration="{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}"
            )
        
        # Wait for the Crawler to finish
        
        
        # table_mappings = {
        #     "COUNTRY_INFO": {
        #         "source": "s3://scheme-ref-dev-data-copy/COUNTRY.txt",
        #         "mapping": {
        #             "COUNTRY_CODE": "COUNTRY_CODE",
        #             "COUNTRY_NAME": "COUNTRY_NAME"
        #         }
        #     }
        # }
        
        # # Glue Job stuff
        glue_job = glue.CfnJob(
            self, "RDMSolutionGlueJob",
            command={
                'name':"glue_poc",
                'pythonVersion':"3",
                'scriptLocation':"s3://etl-glue-scripts/scripts/poc_rdm_etl_cdk.py",
                'extraPythonArguments': '--job-type etl'
            },            
            name="glue_job_poc",
            role=glue_role.role_arn,
            worker_type="G.1X",
            number_of_workers=10,
            glue_version="3.0",
            timeout=2880,
            connections={
                "connections": [glue_connection.ref]
            },
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",
                "--enable-metrics" : "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://etl-glue-scripts/sparkHistoryLogs/",
                "--enable-job-insights": "true",
                "--enable-glue-datacatalog": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": "s3://etl-glue-scripts/temporary/"
            }
        )


        table_locations = [{
            "table_name":"postgres_dev_country_info",
            "s3_path": "COUNTRY.txt"
        }]
        
        # for i, locations in enumerate(table_locations):
        #     table_name = locations["table_name"]
        #     s3_path = locations["s3_path"]

        #     source_dynamic_frame = glue.