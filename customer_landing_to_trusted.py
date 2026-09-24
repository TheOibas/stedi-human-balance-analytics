import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Source (Amazon S3): customer landing zone
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lakehouse-365869683794/customer/landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

# Transform (SQL Query): keep only customers who agreed to share their data for research
SqlQuery0 = """
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
"""
PrivacyFilter_node2 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1},
    transformation_ctx="PrivacyFilter_node2",
)

# Target (Amazon S3 + Data Catalog): customer_trusted
CustomerTrusted_node3 = glueContext.getSink(
    path="s3://stedi-lakehouse-365869683794/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node3",
)
CustomerTrusted_node3.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
CustomerTrusted_node3.setFormat("json")
CustomerTrusted_node3.writeFrame(PrivacyFilter_node2)

job.commit()
