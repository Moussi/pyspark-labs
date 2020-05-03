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

    questions = sc.parallelize([('Aymen', 2), ('Malek', 1), ('Achref', 3)])
    answers = sc.parallelize([('Aymen', 1), ('Malek', 2), ('Achref', 3)])
    aq = questions.union(answers)
    aq.collect()
    #[('Aymen', 2), ('Malek', 1), ('Achref', 3), ('Aymen', 1), ('Malek', 2), ('Achref', 3)]
    aq_join = questions.join(answers)
    aq_join.collect()
    #[('Aymen', (2, 1)), ('Achref', (3, 3)), ('Malek', (1, 2))]
    aq_full_outer_join = answers.fullOuterJoin(questions)
    aq_full_outer_join.collect()
    #[('Aymen', (1, 2)), ('Achref', (3, 3)), ('Malek', (2, 1)), ('Narjes', (None, 6))]
    qa_leftouterjoin = answers.leftOuterJoin(questions)
    qa_leftouterjoin.collect()
    #[('Aymen', (1, 2)), ('Achref', (3, 3)), ('Malek', (2, 1))]
    qa_leftouterjoin = questions.leftOuterJoin(answers)
    qa_leftouterjoin.collect()
    #[('Aymen', (2, 1)), ('Achref', (3, 3)), ('Malek', (1, 2)), ('Narjes', (6, None))]
    # questions.leftOuterJoin(answers) === answers.rightOuterJoin(questions)
    qa_cartestian = questions.cartesian(questions)
    qa_cartestian.collect()
    # [(('Aymen', 2), ('Aymen', 2)), (('Aymen', 2), ('Malek', 1)), (('Aymen', 2), ('Achref', 3)),
    #  (('Aymen', 2), ('Narjes', 6)), (('Malek', 1), ('Aymen', 2)), (('Malek', 1), ('Malek', 1)),
    #  (('Malek', 1), ('Achref', 3)), (('Malek', 1), ('Narjes', 6)), (('Achref', 3), ('Aymen', 2)),
    #  (('Achref', 3), ('Malek', 1)), (('Achref', 3), ('Achref', 3)), (('Achref', 3), ('Narjes', 6)),
    #  (('Narjes', 6), ('Aymen', 2)), (('Narjes', 6), ('Malek', 1)), (('Narjes', 6), ('Achref', 3)),
    #  (('Narjes', 6), ('Narjes', 6))]





