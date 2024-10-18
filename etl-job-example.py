import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Get arguments: source and target buckets
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket'])

# Initialize the Glue context, Spark context, and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 bucket parameters
source_bucket = args['source_bucket']  # e.g., 's3://my-raw-bucket/'
target_bucket = args['target_bucket']  # e.g., 's3://my-discovery-bucket/'

# Recursive read function to process folders
def process_table_folders(input_path, output_path):
    # Read the data from S3 path (for each folder representing a table)
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [input_path],
            "recurse": True
        },
        format="csv", # assuming source files are CSV; change format if needed
        format_options={"withHeader": True}
    )
    
    # Convert DynamicFrame to DataFrame for transformations
    df = dynamic_frame.toDF()

    # Optional: Data Cleaning and Transformations
    # e.g., df = df.withColumn("processed_date", F.current_date())
    
    # Repartition DataFrame to improve Parquet output partitioning if necessary
    # df = df.repartition("column_name")  # Choose appropriate column to partition by

    # Write data to target location in parquet format
    df.write.mode("overwrite").parquet(output_path)

# List all folders (tables) in the source S3 bucket
input_folders = spark._jsc.hadoopConfiguration().listStatus(spark._jsc.hadoopConfiguration().getURI(source_bucket))

for folder in input_folders:
    folder_name = folder.getPath().getName()  # Extract folder name (table name)
    
    # Construct paths for source and destination folders
    input_path = f"{source_bucket}/{folder_name}/"
    output_path = f"{target_bucket}/{folder_name}/"
    
    print(f"Processing folder: {folder_name}")
    
    # Process each folder (table)
    process_table_folders(input_path, output_path)

# Commit job
job.commit()
