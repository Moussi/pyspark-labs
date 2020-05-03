
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

    df_csv_schema = df_csv_schema.withColumn('ViewCount', df_csv_schema['ViewCount'].cast(IntegerType()))

    string_to_list_udf = udf(string_to_list, ArrayType(StringType()))

    df_csv_schema.show(1, truncate=False)
    df_csv_schema.withColumn('Tags', string_to_list_udf(df_csv_schema['Tags'])).show(1, truncate=False)
    df_csv_schema.filter(df_csv_schema['Tags'].isNotNull()).withColumn('Tags', string_to_list_udf(df_csv_schema['Tags'])).select('Id', 'Tags').show(5, truncate=False)
    df_csv_schema.filter(df_csv_schema['Tags'].isNotNull()).withColumn('Tags', string_to_list_udf(df_csv_schema['Tags']))\
        .select(explode(col('Tags'))).distinct().show(5, truncate=False)


