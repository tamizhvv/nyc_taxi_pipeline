from logger import logs
logger,run_id=logs('transform')
from pyspark.sql.functions import col,round,concat_ws,md5

def transform(data):
    logger.info('Transformation started')
    before_count=data.count()
    logger.info(f'Raw row count: {before_count}')
    data=data.dropna(subset=['passenger_count', 'fare_amount', 'trip_distance', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    data=data.filter((data['passenger_count']>0)&(data['fare_amount']>0)&(data['trip_distance']>0))
    data=data.filter(data['tpep_dropoff_datetime']>data['tpep_pickup_datetime'])
    data=data.withColumn('trip_duration_minutes',round((col('tpep_dropoff_datetime') - col('tpep_pickup_datetime')).cast('long') / 60, 2))
    data = data.withColumn('trip_id', md5(concat_ws('|', 
    col('tpep_pickup_datetime').cast('string'),
    col('tpep_dropoff_datetime').cast('string'),
    col('PULocationID').cast('string'),
    col('DOLocationID').cast('string')
)))
    data=data.select('trip_id','tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'payment_type', 'PULocationID', 'DOLocationID', 'trip_duration_minutes')
    data = data.dropDuplicates(['trip_id'])
    after_count=data.count()
    logger.info(f"Transform completed. {after_count} clean records from {before_count} raw records.")
    return data
