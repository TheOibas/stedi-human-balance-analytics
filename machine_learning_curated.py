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

# Source (Data Catalog): step_trainer_trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Source (Data Catalog): accelerometer_trusted
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Transform (SQL Query): inner join readings taken at the same time
SqlQuery0 = """
SELECT DISTINCT *
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a ON s.sensorReadingTime = a.timestamp
"""
SameTimestampJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"step_trainer_trusted": StepTrainerTrusted_node1, "accelerometer_trusted": AccelerometerTrusted_node2},
    transformation_ctx="SameTimestampJoin_node3",
)

# Target (Amazon S3 + Data Catalog): machine_learning_curated
MachineLearningCurated_node4 = glueContext.getSink(
    path="s3://stedi-lakehouse-365869683794/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node4",
)
MachineLearningCurated_node4.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
MachineLearningCurated_node4.setFormat("json")
MachineLearningCurated_node4.writeFrame(SameTimestampJoin_node3)

job.commit()
