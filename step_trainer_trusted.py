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

# Source (Amazon S3): step trainer landing zone
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lakehouse-365869683794/step_trainer/landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1",
)

# Source (Data Catalog): customer_curated
CustomerCurated_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node2",
)

# Transform (SQL Query): inner join on serial number, keep only step trainer columns
SqlQuery0 = """
SELECT DISTINCT s.*
FROM step_trainer_landing s
INNER JOIN customer_curated c ON s.serialNumber = c.serialNumber
"""
CuratedCustomerJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"step_trainer_landing": StepTrainerLanding_node1, "customer_curated": CustomerCurated_node2},
    transformation_ctx="CuratedCustomerJoin_node3",
)

# Target (Amazon S3 + Data Catalog): step_trainer_trusted
StepTrainerTrusted_node4 = glueContext.getSink(
    path="s3://stedi-lakehouse-365869683794/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node4",
)
StepTrainerTrusted_node4.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node4.setFormat("json")
StepTrainerTrusted_node4.writeFrame(CuratedCustomerJoin_node3)

job.commit()
