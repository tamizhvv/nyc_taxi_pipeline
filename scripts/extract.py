import time
import config 
from pyspark.sql import SparkSession
from logger import logs
logger,run_id=logs('extract')

def extract():
    logger.info('Extraction Started')
    spark=SparkSession.builder.appName('nyc_taxi_pipeline').master('local').config("spark.jars", "/home/tamiz/postgresql-42.7.3.jar").getOrCreate()
    retries=config.retry
    for attempts in range(1,retries+1):
        try:
            data=spark.read.parquet(config.source)
            logger.info(f'Extraction successful on attempt{attempts}')
            return data
        except Exception as e:
            if attempts==retries:
                logger.error('Retry limit reached')
                return None
            else:
                logger.warning(f'Attempt {attempts} failed.Retrying in 10 seconds')
                time.sleep(10)

