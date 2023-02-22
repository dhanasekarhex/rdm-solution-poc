from aws_cdk import (
    Stack
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_glue as glue
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3_deployment as s3deploy
from aws_cdk import aws_s3_assets as s3_assets

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

        # Define the Glue Script inline
        script = """
        import sys
        from awsglue.transforms import *
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job


        mapping_dict = [{
            "table_name":"postgres_dev_country_info",
            "s3_path": "s3://scheme-ref-dev-data-copy/COUNTRY.txt",
            "field_mapping":[
                ("COUNTRY_CODE", "string", "COUNTRY_CODE", "string"),
                ("COUNTRY_NAME", "string", "COUNTRY_NAME", "string"),
            ]
        }]

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)


        for item in mapping_dict:
            # Script generated for node S3 bucket
            S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
                format_options={
                    "quoteChar": '"',
                    "withHeader": True,
                    "separator": "|",
                    "optimizePerformance": False,
                },
                connection_type="s3",
                format="csv",
                connection_options={
                    "paths": [item["s3_path"]],
                    "recurse": True,
                },
                transformation_ctx="S3bucket_node1",
            )

            # Script generated for node ApplyMapping
            ApplyMapping_node2 = ApplyMapping.apply(
                frame=S3bucket_node1,
                mappings=item["field_mapping"],
                transformation_ctx="ApplyMapping_node2",
            )

            # Script generated for node PostgreSQL table
            PostgreSQLtable_node3 = glueContext.write_dynamic_frame.from_catalog(
                frame=ApplyMapping_node2,
                database="rdm_solution_glue_database",
                table_name=item["table_name"],
                transformation_ctx="PostgreSQLtable_node3",
            )

        job.commit()


        """

        # Write the script contents to a file
        with open("./poc_rdm_etl_cdk.py", "w") as file:
            file.write(script)

        # Create an asset from the script contents
        script_asset = s3_assets.Asset(
            self, "ETLAsset",
            path="./poc_rdm_etl_cdk.py"
        ).to_string()
        
        # Upload the asset to the bucket
        script_object = s3deploy.BucketDeployment(
            self, "DeployETLScript",
            sources=[s3deploy.Source.asset(script_asset)],
            destination_bucket="etl-glue-scripts/scripts/",
            destination_key_prefix="poc_rdm_etl_cdk.py"
        )
    