import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691661212185 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="region___hive_default_partition__",
    transformation_ctx="AWSGlueDataCatalog_node1691661212185",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691661155454 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1691661155454",
)

# Script generated for node Join
Join_node1691661251297 = Join.apply(
    frame1=AWSGlueDataCatalog_node1691661212185,
    frame2=AWSGlueDataCatalog_node1691661155454,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1691661251297",
)

# Script generated for node joined_cleaned_data
joined_cleaned_data_node1691661442664 = glueContext.getSink(
    path="s3://de-youtube-analytical-bucket",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="joined_cleaned_data_node1691661442664",
)
joined_cleaned_data_node1691661442664.setCatalogInfo(
    catalogDatabase="db_analytics_youtube", catalogTableName="joined_cleaned_data"
)
joined_cleaned_data_node1691661442664.setFormat("glueparquet")
joined_cleaned_data_node1691661442664.writeFrame(Join_node1691661251297)
job.commit()
