import sys
import time
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3
from botocore.exceptions import ClientError

def get_all_devices():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("devices")

    try:
        # Scan the table
        response = table.scan()
        items = response.get('Items', [])

        # If there are more items, continue scanning
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        return items

    except ClientError as e:
        print(f"Error scanning table: {e.response['Error']['Message']}")
        return []

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

devices = get_all_devices()

AmazonS3_node1728119020266 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://BUCKET_NAME"]},
    transformation_ctx="AmazonS3_node1728119020266"
)

current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d")

for device in devices:
    device_id_value = device['device_id']

    SqlQuery0 = f'''
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
        AND device_id = '{device_id_value}'
    GROUP BY device_id
    ORDER BY device_id ASC
    '''
    SQLQuery_node1728119425488 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping = {"myDataSource":AmazonS3_node1728119020266}, transformation_ctx = "SQLQuery_node1728119425488")

    output_df = SQLQuery_node1728119425488.toDF().repartition(1)
    
    output_df.show()

    time.sleep(300)

job.commit()