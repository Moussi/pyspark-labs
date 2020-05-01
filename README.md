## 1/ On travaille TOUJOURS dans un virtualenv
````
virtualenv -p python3 py37-labs
````
ou 
````
python -m venv py37-labs
conda create --name py37-env python=3.7
````
Puis (sous Linux)
````
cd env
. ./bin/activate
````

## 2/ Créons notre strucure de projet
Structure du projet classique...
* README => fortement suggéré par gitlab / github
* LICENSE => fortement suggéré par gitlab / github
* .gitignore => prennons celui de github pour le langage python et ajoutons les spécificités de nos éditeurs et évitons de commiter notre vitualenv
* setup.py et setup.cfg pour installer notre projet
* requirements.txt
* requirements-dev.txt si nécessaire
* doc
* tests
* et les modules...

On installe le projet en local avec pip. Pour pouvoir bosser sans réinstaller, on utilise l'option -e (--editable), le mode développement :
````
pip install -e .
````

Et les dépendance de développement (pytest, black)
````
pip install -r requirements-dev.txt
````

## 3/ Installons un éditeur de code
PyCharm : https://www.jetbrains.com/pycharm/download/
ou VSCode

## 4/ Le main du projet...

Le main est dans le fichier qualify_referers.py

Et on l'ajoute au setup.cfg :
````
[options.entry_points]
console_scripts =
    qualify_referers = qualify_referers:main_method
```` 
Et on réinstalle
````
pip install -U -e .
````

# PySpark
## Spark memory and executors optimization
https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html

## Partitions
* repartition() specifies new number of partitions up or down
* coalesce() uses existing partitions, only decreases, no shuffles
* glom()
* be careful of data sqoop: not equitable partitions.

## aggregation
* groupByKey => shuffling ++++ ==> avoid this in large datasets
* reduceByKey => calculate locally and shuffle results
* aggregationByKey = specify combining function (within partition) and merging function (across partition)
* combineByKey
* countByKey
* histogram: grouping data by buckets

## Caching and persistence data
* cache() is equivalent to persist(MEMORY_ONLY)


## env vars
export SPARK_HOME=/path/to/spark
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export SPARK_DIST_CLASSPATH=/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/share/hadoop/yarn/*:/usr/local/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/contrib/capacity-scheduler/*.jar


