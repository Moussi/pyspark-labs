from pyspark import SparkConf
from pyspark.sql import SparkSession

from labs.config import spark
import labs.logging.logger as logging

logger = logging.getLogger('labs.helpers.spark')


class Session:

    def __init__(self, name, **spark_options):
        spark_conf = dict(spark.session.toDict())

        if spark_options:
            spark_conf.update(spark_options)

        logger.info(f"{spark_conf}")
        spark_conf = SparkConf.setAll(spark_conf.items)
        self.session = SparkSession.Builder.appName(name).master(spark_conf["master"]).config(spark_conf).getOrCreate()

    def __enter__(self):
        return self.session

    def __exit__(self, type, value, traceback):
        self.session.stop()




