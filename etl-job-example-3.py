import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Get the job name from arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the Glue context, Spark context, and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Hardcoded S3 bucket parameters
source_bucket = "my-raw-bucket"           # e.g., 'my-raw-bucket'
target_bucket = "my-discovery-bucket"     # e.g., 'my-discovery-bucket'
source_bucket_prefix = "xyz_schema/"      # e.g., 'xyz_schema/'
target_bucket_prefix = "nested_discovery/" # e.g., 'nested_discovery/' (can be empty if no prefix needed)

# Function to extract folder name from the full file path
def extract_folder_name(file_path, source_bucket_prefix):
    # Assuming file paths are like s3://bucket/xyz_schema/folder_name/filename.csv
    # We want to extract the folder_name part
    path_parts = file_path.split(source_bucket_prefix)[1].split('/')
    return path_parts[0] if len(path_parts) > 1 else None

# Read all files from the source bucket prefix in one go
input_path = f"s3://{source_bucket}/{source_bucket_prefix}/"
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [input_path],
        "recurse": True  # Recursively read all files within the source_bucket_prefix
    },
    format="csv",  # assuming source files are CSV; change format if needed
    format_options={"withHeader": True}
)

# Convert DynamicFrame to DataFrame for transformations
df = dynamic_frame.toDF()

# Add a new column to extract folder (table) name from the file path
df = df.withColumn("folder_name", F.input_file_name()) \
       .withColumn("folder_name", F.udf(lambda file_path: extract_folder_name(file_path, source_bucket_prefix))(F.col("folder_name")))

# Filter out any rows where folder_name could not be extracted (for safety)
df = df.filter(F.col("folder_name").isNotNull())

# Ensure the "update_time" column is of TimestampType
df = df.withColumn("update_time", F.col("update_time").cast("timestamp"))

# Extract year, month, day, and hour from the "update_time" column
df = df.withColumn("year", F.year(F.col("update_time"))) \
       .withColumn("month", F.month(F.col("update_time"))) \
       .withColumn("day", F.dayofmonth(F.col("update_time"))) \
       .withColumn("hour", F.hour(F.col("update_time")))

# Write each table's data (each folder) into a separate folder in the target bucket, partitioned by year, month, day, hour
folder_names = df.select("folder_name").distinct().collect()

for row in folder_names:
    folder = row['folder_name']
    
    # Filter data for each folder (table)
    df_table = df.filter(F.col("folder_name") == folder).drop("folder_name")

    # Target path for writing, including optional target_bucket_prefix
    if target_bucket_prefix:
        output_path = f"s3://{target_bucket}/{target_bucket_prefix}{folder}/"
    else:
        output_path = f"s3://{target_bucket}/{folder}/"

    # Write DataFrame to target folder in Parquet format partitioned by "year", "month", "day", and "hour"
    df_table.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(output_path)

# Commit job
job.commit()
