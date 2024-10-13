import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job

# Function to trigger SNS notification
def sns_msg_trigger_func(subject="From Glue", message=""):
    """Sends an SNS notification with the given subject and message."""
    if not message:
        return None  # No message, no action needed

    sns_message_content = (
        f"Hello User,\n\n{message}\nRegards,\nBMW E2E DataMigration Team"
    )

    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=sns_message_content,
        Subject=subject[:100]  # Subject truncated to meet SNS limit
    )
    return response

# Retrieve job parameters
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "sns_topic_arn", "athena_extract_path", "transform_data_path"]
)
sns_topic_arn = args["sns_topic_arn"]
athena_extract_path = args["athena_extract_path"]
transform_data_path = args["transform_data_path"]

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize AWS clients
sns_client = boto3.client("sns")
s3 = boto3.client("s3")

subject, message = "", ""

try:
    # Read data from the Athena extract in S3
    athena_extract_df = (
        glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="parquet",
            connection_options={"paths": [athena_extract_path]},
        ).toDF()
    )

    # (Optional) Apply transformations on the DataFrame
    transformed_df = athena_extract_df  # Placeholder for transformations

    # Write the transformed data back to S3 in Parquet format
    transformed_df.write.mode("overwrite").parquet(transform_data_path)

except Exception as err:
    # Handle exceptions and trigger SNS notification
    subject = "Failure CRM Transform Job: aot_transform"
    message = f"Error occurred: {str(err)}"
    print("SNS Message:", message)
    response = sns_msg_trigger_func(subject, message)
    print("SNS Response:", response)
    raise  # Rethrow the exception to ensure failure is logged

finally:
    # Ensure the job is committed, even if an exception occurs
    job.commit()
