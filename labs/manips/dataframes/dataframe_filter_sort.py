
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def string_to_list(str_list):
    if str_list is None or str_list == '' or len(str_list)<2 :
        return []
    elements = str_list[1: len(str_list)-1]
    return list(elements.split(','))


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

    df = df_csv_schema.select('Id', when(col('PostTypeId') == 1, 'Question').otherwise('Answer').alias('PostType'))\
        .groupBy('PostType').count()
    df_sort1 = df.orderBy(col('count'), ascending=True)
    df_sort1.show()
    df_sort2 = df.sort(col('count'), ascending=True)
    df_sort2.show()
    df_sort3 = df.orderBy(asc('count'), desc('PostType'))
    df_sort3.show()