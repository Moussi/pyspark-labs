
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[3]').appName('dataframes').getOrCreate()

    df1 = ss.createDataFrame([(2, 'Aymen'), (3, 'Malek'), (4, 'Achref')]).toDF('id', 'name') # with list
    df2 = ss.createDataFrame({(2, 'Aymen'), (3, 'Malek'), (4, 'Achref')}) # with dict
    df3 = ss.createDataFrame([Row(2, 'Aymen'), Row(3, 'Malek'), Row(4, 'Achref')]) # with Row
    print(df1.collect())
    print(df2.collect())
    print(df3.collect())

    df_csv = ss.read.csv('/home/moussi/Desktop/Projects/pyspark-cloudera-labs/pyspark-lab/datasets/cloudera/stackexchange/posts_all_csv')

    schema = StructType([
        StructField("Id", IntegerType()),
        StructField("PostTypeId", IntegerType()),
        StructField("AcceptedAnswerId", IntegerType()),
        StructField("CreationDate", TimestampType()),
        StructField("Score", IntegerType()),
        StructField("ViewCount", StringType()),
        StructField("OwnerUserId", IntegerType()),
        StructField("LastEditorUserId", IntegerType()),
        StructField("LastEditDate", TimestampType()),
        StructField("Title", StringType()),
        StructField("LastActivityDate", TimestampType()),
        StructField("Tags", StringType()),
        StructField("AnswerCount", IntegerType()),
        StructField("CommentCount", IntegerType()),
        StructField("FavouriteCount", IntegerType())
    ])
    df_csv_schema = ss.read.schema(schema).csv('/home/moussi/Desktop/Projects/pyspark-cloudera-labs/pyspark-lab/datasets/cloudera/stackexchange/posts_all_csv')
    df_csv_schema.show(2, truncate=False)
