import os
from dotenv import load_dotenv
load_dotenv()
source=os.getenv('SOURCE')
retry=int(os.getenv('RETRY'))
aws_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key=os.getenv('AWS_SECRET_ACCESS_KEY')
aws_bucket=os.getenv('AWS_BUCKET_NAME')
aws_region=os.getenv('AWS_REGION')
pg_host=os.getenv('PG_HOST')
pg_db=os.getenv('PG_DATABASE')
pg_user=os.getenv('PG_USER')
pg_password=os.getenv('PG_PASSWORD')
pg_port=os.getenv('PG_PORT')
