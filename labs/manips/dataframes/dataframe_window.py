from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *


def string_to_list(str_list):
    if str_list is None or str_list == '' or len(str_list)<2 :
        return []
    elements = str_list[1: len(str_list)-1]
    return list(elements.split(','))


def total_join_op(accumulator: list, element):
    return accumulator.append(element[1])


def total_comb_op(accumulator: list, element):
    return accumulator.append(element)


if __name__ == '__main__':
    ss = SparkSession.builder.master('local[3]').appName('dataframes').getOrCreate()

    df_ar = ss.read.option("header", True).csv('/home/moussi/Desktop/Projects/pyspark-cloudera-labs/scikit learning/tn_ar.csv')
    df_fr = ss.read.option("header", True).csv('/home/moussi/Desktop/Projects/pyspark-cloudera-labs/scikit learning/tn_fr_brut.csv')
    df_ar = df_ar.select("state", "department", "cities").withColumn("cities", split(col("cities"), "، "))\
        .withColumnRenamed("state", "state_ar").withColumnRenamed("department", "department_ar")
    df_fr = df_fr.select("state", "departments").withColumn("departments", split(col("departments"), " – "))
    df_ar.show(5, truncate=False)

    # zero = []
    # df_tn_states = df.select("state","department").groupby("state").agg(collect_set("department").alias("departments"))

    # rdd = df_tn_states.aggregateByKey(zero, total_join_op, total_comb_op)
    # print(rdd.take(2))
    # df_tn_renamed = df_tn.withColumnRenamed("Gouvernorat", "state").withColumnRenamed("Délégations", "department")
    flatten_df_fr = ss.createDataFrame(df_fr.rdd.flatMap(lambda x: ([(x[0], ele) for ele in x[1]])).collect()).toDF('state', 'department')
    # df_tn_renamed.coalesce(1).write.json("/home/moussi/Desktop/Projects/pyspark-cloudera-labs/scikit learning/fr_json")
    # df_tn_states.coalesce(1).write.json("/home/moussi/Desktop/Projects/pyspark-cloudera-labs/scikit learning/ar_states_json")

    flatten_df_fr = flatten_df_fr.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
    df_ar = df_ar.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))

    df_fr_un = flatten_df_fr.join(df_ar, on=["row_index"]).drop("row_index").select("state", "department", "cities")
    flatten_df_fr.show(5,truncate=False)
    df_fr_un.show(5,truncate=False)
    df_fr_un.coalesce(1).write.json("/home/moussi/Desktop/Projects/pyspark-cloudera-labs/scikit learning/fr_states_json")