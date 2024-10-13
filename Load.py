import sys
import boto3
import math
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import expr, coalesce

# Function to trigger SNS notification
def sns_msg_trigger_func(subject="From Glue", message=""):
    """Publishes an SNS notification with the given subject and message."""
    if message:
        sns_message_content = f"Hello User,\n\n{message}\nRegards,\nBMW E2E DataMigration Team"
        try:
            response = sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=sns_message_content,
                Subject=subject[:100]
            )
            print("SNS Message published successfully.")
            return response
        except Exception as err:
            print(f"Failed to publish SNS message: {str(err)}")
    return None

# Retrieve job parameters
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "sns_topic_arn", "transform_data_path", "output_path", "target_path"]
)
sns_topic_arn = args["sns_topic_arn"]
transform_data_path = args["transform_data_path"]
output_path = args["output_path"]
target_path = args["target_path"]

# Extract bucket and key details
output_bucket_name = output_path.split("/")[2]
output_file_key = "/".join(output_path.split("/")[3:])
target_bucket_name = target_path.split("/")[2]
target_file_key = "/".join(target_path.split("/")[3:])

print(f"Output Path: {output_path}, Bucket: {output_bucket_name}, Key: {output_file_key}")
print(f"Target Path: {target_path}, Bucket: {target_bucket_name}, Key: {target_file_key}")

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize AWS clients
sns_client = boto3.client("sns")
s3_client = boto3.client("s3")
s3_resource = boto3.resource('s3')

try:
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Read transformed data from S3
    transformed_df = (
        glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="parquet",
            connection_options={"paths": [transform_data_path]},
        ).toDF()
    )

    print("Transformed DataFrame loaded successfully.")

    # Define column transformations
    column_length_list = [
        ("VIN", 17), ("SERIAL_NUMBER", 7), ("PRODUCT_TYPE", 1), ("UCID", 10),
        ("CURRENT_IND", 1), ("FORMER_IND", 1), ("PREFIX", 12), ("FIRSTNAME", 15),
        ("MIDDLENAME", 1), ("LASTNAME", 20), ("SUFFIX", 58), ("ADDRESS", 47),
        ("PERSONMAILINGCITY", 28), ("PERSONMAILINGSTATECODE", 3), ("ZIPCODE", 10),
        ("PERSONMAILINGCOUNTRYCODE", 2), ("HOMEPHONE", 10), ("HOMEEXT", 4),
        ("WORKPHONE", 10), ("WORKEXT", 167), ("LASTMODIFIEDDATE", 49), ("ORPHAN_IND", 27)
    ]

    # Apply transformations
    processed_df = transformed_df.select(
        *[
            expr(f"substr(rpad(coalesce({col_name}, ''), {length}, ' '), 1, {length})").alias(col_name)
            for col_name, length in column_length_list
        ]
    ).select(
        expr(f"substr(concat_ws('', {', '.join([col for col, _ in column_length_list])}), 1, {sum(length for _, length in column_length_list)})").alias("Output")
    )

    # Partition the output into smaller files
    record_count = processed_df.count()
    partition_count = math.ceil(record_count / 2_500_000)
    print(f"Total Record Count: {record_count}, Partition Count: {partition_count}")

    # Write data to S3 in text format with gzip compression
    for i in range(1, partition_count + 1):
        write_df = processed_df.limit(2_500_000)
        output_partition_path = f"{output_path}/{i}"
        print(f"Writing partition {i} to {output_partition_path}")
        
        write_df.coalesce(1).write.mode("overwrite").option("header", "false") \
            .option("compression", "gzip").text(output_partition_path)
        
        processed_df = processed_df.subtract(write_df)  # Remove written records

    # Rename and move files to the target bucket
    output_files = [obj.key for obj in s3_resource.Bucket(output_bucket_name).objects.filter(Prefix=output_file_key + "/")]
    for n, file in enumerate(output_files, 1):
        target_filename = f"AOT_VIEW_Report_View_{current_date}_{n:03}.txt.gz"
        s3_resource.meta.client.copy(
            {"Bucket": output_bucket_name, "Key": file},
            target_bucket_name,
            f"{target_file_key}/{target_filename}"
        )
        print(f"Copied: {file} to {target_filename}")
        s3_resource.Object(output_bucket_name, file).delete()
        print(f"Deleted: {file}")

    # Trigger success SNS notification
    subject = "Success CRM Data loaded to S3_AOT_VIEW"
    message = f"Data successfully loaded to {output_path}"

except Exception as err:
    # Handle exceptions and trigger failure SNS notification
    subject = "Failure CRM Data load S3_AOT_VIEW"
    message = f"Error occurred: {str(err)}"
    print(f"Error during data processing: {message}")

# Send SNS notification
response = sns_msg_trigger_func(subject, message)
print(f"SNS Message: {message}")
print(f"SNS Response: {response}")

# Raise exception if the job failed
if subject == "Failure CRM Data load S3_AOT_VIEW":
    raise Exception(message)

# Commit the Glue job
job.commit()
