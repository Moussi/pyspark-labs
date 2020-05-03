
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

    # dropna any(default)/how/subsets
    df = df_csv_schema.dropna(how='any').select('Title').show(5)
    df = df_csv_schema.dropna(subset='Title').select('Title').show(5, truncate=False)

    # replace
    df_replace = df_csv_schema.select('Id', 'Title').replace('How can I change the default indentation based on filetype?', 'Redacted').show(5)
    df_replace = df_csv_schema.select('Id', 'Title').fillna('N/A').show(5)

    # Handling corrupt records
    df = ss.read.json('path/to/josn', mode='PERMISSIVE', columnNameOfCorruptRecord='Invalid') # read corrupted record and putit in Invalid column
    df = ss.read.json('path/to/josn', mode='DROPMALFORMED') # Drop corrupted records and read the rest
    df = ss.read.json('path/to/josn', mode='FAILFAST') # raise exception if one or more records are corrupted