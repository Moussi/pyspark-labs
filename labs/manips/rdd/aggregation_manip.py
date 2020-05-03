from pyspark import SparkContext


def max_join_op(accumulator, element):
    if accumulator[1] > element[1]:
        return accumulator
    else:
        return element


def max_comb_op(accumulator, element):
    if accumulator[1] > element[1]:
        return accumulator
    else:
        return element


def total_join_op(accumulator, element):
    return accumulator + element[1]


def total_comb_op(accumulator, element):
    return accumulator + element


if __name__ == '__main__':

    sc = SparkContext(master="local[3]", appName="manip1").getOrCreate()
    # reduce by key vs groupBy key
    questions = sc.parallelize([('Aymen', 2), ('Malek', 1), ('Achref', 3)])
    answers = sc.parallelize([('Aymen', 1), ('Malek', 2), ('Achref', 3)])

    qa = questions.union(answers)

    qa_reduce = qa.reduceByKey(lambda x, y: (x, y))
    qa_group = qa.groupByKey()
    print(qa_reduce.collect())
    print(qa_group.map(lambda x: (x[0], list(x[1]))).collect())

    # aggregate by key
    student_rdd = sc.parallelize([
        ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
        ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)], 3)
    student_subjects_marks = student_rdd.map(lambda element: (element[0], (element[1], element[2])))
    print(student_subjects_marks.collect())
    best_zero_val = ("", 0)
    total_zero_val = 0
    student_best_marks = student_subjects_marks.aggregateByKey(best_zero_val, max_join_op, max_comb_op)
    student_total_marks = student_subjects_marks.aggregateByKey(total_zero_val, total_join_op, total_comb_op)

    print(student_best_marks.collect())
    print(student_total_marks.collect())

    print(student_total_marks.map(lambda x: x[1]).histogram([0, 260, 300, 330, 350]))
