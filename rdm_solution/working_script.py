script = """import sys
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