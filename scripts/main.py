import config
from extract import extract
from transform import transform
from load import load
from upload_to_s3 import upload_to_s3
from logger import logs
logger,run_id=logs('main')

def run_pipeline():
    logger.info('Pipeline Started')
    raw=extract()
    if raw is None:
        logger.error('Pipeline aborted at extract')
        return
    cleaned=transform(raw)
    upload_to_s3(cleaned)
    loaded=load(cleaned)
    logger.info('Pipeline Completed')
if __name__=='__main__':
    run_pipeline()
