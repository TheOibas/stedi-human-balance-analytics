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

# Source (Amazon S3): accelerometer landing zone
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lakehouse-365869683794/accelerometer/landing/"], "recurse": True},
    transformation_ctx="AccelerometerLanding_node1",
)

# Source (Data Catalog): customer_trusted
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node2",
)

# Transform (SQL Query): inner join on email, keep only accelerometer columns
SqlQuery0 = """
SELECT DISTINCT a.*
FROM accelerometer_landing a
INNER JOIN customer_trusted c ON a.user = c.email
"""
CustomerPrivacyJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"accelerometer_landing": AccelerometerLanding_node1, "customer_trusted": CustomerTrusted_node2},
    transformation_ctx="CustomerPrivacyJoin_node3",
)

# Target (Amazon S3 + Data Catalog): accelerometer_trusted
AccelerometerTrusted_node4 = glueContext.getSink(
    path="s3://stedi-lakehouse-365869683794/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node4",
)
AccelerometerTrusted_node4.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node4.setFormat("json")
AccelerometerTrusted_node4.writeFrame(CustomerPrivacyJoin_node3)

job.commit()
