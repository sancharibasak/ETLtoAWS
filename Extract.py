import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Function to trigger SNS notification
def sns_msg_trigger_func(subject="From Glue", message=""):
    if not message:
        return None  # No message, no action needed

    sns_message_content = (
        f"Hello User,\n\n{message}\nRegards,\nBMW E2E DataMigration Team"
    )

    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=sns_message_content,
        Subject=subject[:100]  # SNS subject limit is 100 characters
    )
    return response

# Retrieve job parameters
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "sns_topic_arn", "athena_extract_path", "TempDir"]
)
sns_topic_arn = args["sns_topic_arn"]
athena_extract_path = args["athena_extract_path"]
temp_dir = args["TempDir"]

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
    # Extract data from Athena
    athena_df = (
        glueContext.read.format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option(
            "AwsCredentialsProviderClass",
            "com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
        .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
        .option("dbtable", "AwsDataCatalog.wholesale_crm.aot_extract")
        .option("S3OutputLocation", temp_dir)
        .load()
    )

    # Write data to the specified location in parquet format
    athena_df.write.mode("overwrite").parquet(athena_extract_path)

except Exception as err:
    # Handle exceptions and trigger SNS notification
    subject = "Failure CRM Extract Job: aot_extract"
    message = f"Error occurred: {str(err)}"
    print("SNS Message:", message)
    response = sns_msg_trigger_func(subject, message)
    print("SNS Response:", response)
    raise  # Rethrow the exception after sending notification

finally:
    # Ensure the job is committed even if exceptions occur
    job.commit()
