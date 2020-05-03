from pyspark import SparkContext
import os


def badge_partitionner(badge):
    return hash(badge)


def count_badges(iterator):
    total = 0
    for elem in iterator:
        total += 1
    yield total


def generator_ex1(lista):
    for ele in lista:
        yield ele


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "/home/moussi/Desktop/Projects/pyspark-cloudera-labs/spark-2.4.5-bin-without-hadoop"

    sc = SparkContext(master="local[3]", appName="manip1").getOrCreate()
    rdd1 = sc.textFile("../../datasets/cloudera/stackexchange/posts_all_csv")
    rdd_columns = rdd1.map(lambda line: line.split(','))
    rdd_pair = rdd_columns.map(lambda x: (x[2], x))
    rdd_partitionned = rdd_pair.partitionBy(5, badge_partitionner)
    # rdd_partitionned.saveAsTextFile('/home/moussi/Desktop/Data/partition2')
    rdd_partitionned_count = rdd_partitionned.mapPartitions(count_badges)
    print(rdd_partitionned_count.collect())
