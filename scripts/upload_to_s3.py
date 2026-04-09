import config
import boto3
from logger import logs
logger,run_id=logs('upload_to_s3')
import os
from datetime import datetime

def upload_to_s3(data):
    timestamp=datetime.now().strftime('%Y%m%d-%H:%M:%S')
    local_path = f"/home/tamiz/nyc_taxi_pipeline/data/temp_{timestamp}"
    data.write.mode("overwrite").parquet(local_path)

    s3=boto3.client('s3',
    aws_access_key_id=config.aws_key_id,
    aws_secret_access_key=config.aws_secret_key,
    region_name=config.aws_region
    )

    for filename in os.listdir(local_path):
        if filename.endswith('.parquet'):
            local_file = os.path.join(local_path, filename)
            s3_key = f"nyc_taxi/{timestamp}/{filename}"
            s3.upload_file(local_file, config.aws_bucket, s3_key)

    logger.info(f'Upload complete to s3://{config.aws_bucket}/nyc_taxi/{timestamp}/')
    return f"nyc_taxi/{timestamp}/"