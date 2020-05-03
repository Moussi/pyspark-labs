from pyspark.sql import SparkSession
from os.path import abspath
if __name__ == '__main__':
    # add jdbc connector to classpath
    warehouse_path = abspath("path/to/warehouse")
    ss = SparkSession.builder.master("local[*]").appName("hive")\
        .config("spark.sql.warehouse.dir", warehouse_path)\
        .enableHiveSupport().getOrCreate()

    df = ss.sql("select * from table")
    df.show()