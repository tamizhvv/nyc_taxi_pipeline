import config
import psycopg2
from logger import logs
logger,run_id=logs('load')
jdbc_url=f'jdbc:postgresql://{config.pg_host}:{config.pg_port}/{config.pg_db}'
properties = {
    "user": config.pg_user,
    "password": config.pg_password,
    "driver": "org.postgresql.Driver"
    }

def load(data):
    logger.info('Load Started')
    data.write.jdbc(url=jdbc_url, table="staging_trips", mode="overwrite", properties=properties)
    logger.info('Staging write is completed')
    connection=None
    try:
        connection=psycopg2.connect(
            host=config.pg_host,
            database=config.pg_db,
            user=config.pg_user,
            password=config.pg_password,
            port=config.pg_port
        )
        cursor=connection.cursor()
        merge_sql='''INSERT INTO trips
        SELECT * FROM staging_trips
        ON CONFLICT (trip_id) DO UPDATE SET
        tpep_pickup_datetime = EXCLUDED.tpep_pickup_datetime,
        tpep_dropoff_datetime = EXCLUDED.tpep_dropoff_datetime,
        passenger_count = EXCLUDED.passenger_count,
        trip_distance = EXCLUDED.trip_distance,
        fare_amount = EXCLUDED.fare_amount,
        total_amount = EXCLUDED.total_amount,
        payment_type = EXCLUDED.payment_type,
        PULocationID = EXCLUDED.PULocationID,
        DOLocationID = EXCLUDED.DOLocationID,
        trip_duration_minutes = EXCLUDED.trip_duration_minutes''';

        cursor.execute(merge_sql)
        cursor.execute("TRUNCATE TABLE staging_trips")
        logger.info('Staging cleared')
    finally:
        if connection:
            connection.commit()
            cursor.close()
            connection.close()