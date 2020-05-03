
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    ss = SparkSession.builder.master('local[3]').appName('dataframes').getOrCreate()
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
    df_csv_schema = ss.read.schema(schema).csv('/home/moussi/Desktop/Projects/pyspark-cloudera-labs/pyspark-lab/datasets/cloudera/stackexchange/posts_all_csv');

    df_csv_schema = df_csv_schema.withColumn('ViewCount', df_csv_schema['ViewCount'].cast(IntegerType()))

    df_csv_schema.printSchema()

    # canonical
    df1 = df_csv_schema.select(df_csv_schema['Title'])
    df1.show(1, truncate=False)
    # Dot Notation case sensitive !!!
    df2 = df_csv_schema.select(df_csv_schema.Title)
    df2.show(1, truncate=False)
    # col function => from pyspark.sql.functions import *
    df3 = df_csv_schema.select(col('Title'))
    df3.show(1, truncate=False)
    # text be careful of literals
    df4 = df_csv_schema.select('Title')
    df4.show(1, truncate=False)
    # mixture
    df5 = df_csv_schema.select('Title', col('id'), df_csv_schema.Tags, df_csv_schema.Score).withColumn('High Score', df_csv_schema['Score']*1000)
    df6 = df_csv_schema.select('Title', col('id'), df_csv_schema.Tags, (df_csv_schema['Score']*1000).alias('High Scoreee'))
    df5.show(1, truncate=False)
    df6.show(1, truncate=False)
