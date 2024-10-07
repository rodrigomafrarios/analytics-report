import sys
from datetime import datetime
from awsglue.transforms import *
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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d")

AmazonS3_node1728119020266 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://BUCKET_NAME"]},
    transformation_ctx="AmazonS3_node1728119020266"
)

SqlQuery0 = '''
SELECT 
    device_id as `Device ID`,
    ROUND(MIN(CAST(_value AS DOUBLE)), 2) AS Min,
    '' AS Safety,
    ROUND(MAX(CAST(_value AS DOUBLE)), 2) AS Max,
    ROUND(AVG(CAST(_value AS DOUBLE)), 2) AS Mean
FROM myDataSource
WHERE 
    unix_timestamp(_time, 'yyyy-MM-dd HH:mm:ss.SSSSSS') >= unix_timestamp(date_sub(current_timestamp(), 7))
    AND CAST(_value AS DOUBLE) >= 0
    AND CAST(_value AS DOUBLE) < 150
GROUP BY device_id
ORDER BY device_id ASC
'''
SQLQuery_node1728119425488 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping = {"myDataSource":AmazonS3_node1728119020266}, transformation_ctx = "SQLQuery_node1728119425488")

output_df = SQLQuery_node1728119425488.toDF().repartition(1)

# output_path = f"s3://glue-job-query-output/csv/{formatted_datetime}/"

# output_df.write \
#     .mode("overwrite") \
#     .format("csv") \
#     .option("header", True) \
#     .save(output_path)

output_df.show()

job.commit()