import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Get arguments: source and target buckets, and prefixes
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket', 'source_bucket_prefix', 'target_bucket_prefix'])

# Initialize the Glue context, Spark context, and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 bucket parameters
source_bucket = args['source_bucket']  # e.g., 'my-raw-bucket'
target_bucket = args['target_bucket']  # e.g., 'my-discovery-bucket'
source_bucket_prefix = args['source_bucket_prefix']  # e.g., 'xyz_schema/'
target_bucket_prefix = args['target_bucket_prefix']  # e.g., 'nested_discovery/' (can be optional)

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

# Write each table's data (each folder) into a separate folder in the target bucket
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

    # Write DataFrame to target folder in Parquet format
    df_table.write.mode("overwrite").parquet(output_path)

# Commit job
job.commit()
