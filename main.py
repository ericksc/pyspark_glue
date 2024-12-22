import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Parse the arguments passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'DEST_PATH'])

# Initialize GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a Job object
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input and output S3 paths
source_path = args['SOURCE_PATH']
dest_path = args['DEST_PATH']

# Step 1: Read data from S3 (CSV format)
print(f"Reading data from {source_path}")
input_df = spark.read.option("header", True).csv(source_path)

# Step 2: Data transformation (example: filter rows where 'age' > 25)
print("Transforming data...")
transformed_df = input_df.filter("age > 25")

# Step 3: Write the transformed data back to S3 in Parquet format
print(f"Writing transformed data to {dest_path}")
transformed_df.write.mode("overwrite").parquet(dest_path)

# Finalize Glue job
job.commit()
print("Job completed successfully.")
