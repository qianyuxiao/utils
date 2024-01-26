from pyspark.sql import SparkSession
import logging
from google.cloud import storage

def configure_spark_session(
    app_name = 'undefined_app',
    num_instances = 4,
    num_shuffles = 200 ):

    num_instances = num_instances
    num_shuffles = num_shuffles
    app_name = app_name

    return (SparkSession.builder
            .appName(app_name)
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar")
            .config("spark.local.dir", "/home/qianyucazelles/qianyu/reviews_translation/.cache")
            .config("spark.executor.memory", "12g")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.instances", num_instances)
            .config("spark.sql.shuffle.partitions", num_shuffles)
            .getOrCreate())

def log_message(message, level='info'):
    log_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    log_level = log_levels.get(level.lower(), logging.INFO)
    log_format = '==========\n%(levelname)s: %(message)s\n=========='
    
    logging.basicConfig(level=log_level, format=log_format)
    logger = logging.getLogger()

    logger.log(log_level, message)
    
def check_duplicates(df, cols, threhold=0):
    nr_dups = df.count() - df.select(*cols).distinct().count()
    perc_dups = round(nr_dups/df.count()*100,2)
    if nr_dups>0:
        log_message(f"{nr_dups} duplications found in {cols}.")
        if perc_dups>threhold:
            raise ValueError(f'{perc_dups}% duplication found in {cols}')
            

def download_txt_to_list( blob_name, bucket_name='qianyu_bigdata'):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # Create an empty list to store the items
    blob = bucket.blob(blob_name)

    # Download the contents of the blob as text
    content = blob.download_as_text()

    # Split the content into lines and store it in a list
    lines = content.split(',\r\n')
    data_list = [item.strip("' ").rstrip("' ") for item in lines]
    client.close()

    return data_list