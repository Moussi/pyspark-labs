from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as func

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
    df_csv_schema = ss.read.schema(schema).csv(
        '/home/moussi/Desktop/Projects/pyspark-cloudera-labs/pyspark-lab/datasets/cloudera/stackexchange/posts_all_csv');

    df = df_csv_schema.select('Id', when(col('PostTypeId') == 1, 'Question').otherwise('Answer').alias('PostType')) \
        .groupBy('PostType').count()
    df.show(5)

    df2 = df_csv_schema.select('Id', when(col('PostTypeId') == 1, 'Question').otherwise('Answer').alias('PostType'), 'Score') \
        .groupBy('PostType').agg(func.avg('Score'), func.count('Id'))
    df2.show(5)

    ## Joining
    ask_questions_df = df_csv_schema.where('PostTypeId == 1').select('OwnerUserId').distinct()
    answer_questions_df = df_csv_schema.where('PostTypeId == 2').select('OwnerUserId').distinct()

    ask_questions_df.show(5)
    answer_questions_df.show(5)
    print(f'ask {ask_questions_df.count()}')
    print(f'answer {answer_questions_df.count()}')

    ask_answer_questions_df = ask_questions_df.join(answer_questions_df, ask_questions_df.OwnerUserId == answer_questions_df.OwnerUserId)
    ask_answer_questions_df.show(5)
    print(f'answer_ask {ask_answer_questions_df.count()}')
