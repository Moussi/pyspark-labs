from pyspark import SparkContext
import os

if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "/home/moussi/Desktop/Projects/pyspark-cloudera-labs/spark-2.4.5-bin-without-hadoop"

    sc = SparkContext(master="local[3]", appName="manip1").getOrCreate()
    rdd1 = sc.textFile("../../datasets/cloudera/stackexchange/posts_all_csv")
    rdd2 = sc.parallelize([[1, 2, 3], [4], [5]])
    rdd_lines = rdd1.flatMap(lambda line: line)
    rdd_lines2 = rdd2.flatMap(lambda line: line)
    print(rdd_lines.collect())
    print(rdd_lines2.collect())
    rdd3 = sc.textFile("../../datasets/text/speech.txt")
    rdd_lines3 = rdd3.flatMap(lambda line: line.split(" "))
    rdd_count = rdd_lines3.map(lambda x: (x, 1))
    word_count = rdd_count.reduceByKey(lambda x, y:x+y)
    print(word_count.collect())
    sortbykey_rdd = word_count.sortByKey()
    sortby_rdd = word_count.sortBy(lambda x: -x[1])
    print(sortbykey_rdd.collect())
    print(sortby_rdd.collect())
    print(rdd_lines3.zipWithIndex().collect())