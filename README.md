## virtualenv
````
virtualenv -p python3 py37-labs
````
or
````
python -m venv py37-labs
conda create --name py37-env python=3.7
````
activate (Linux)
````
cd env
. ./bin/activate
````

option -e (--editable), for devlepment mode:  
````
pip install -e .
````
````
pip install -r requirements-dev.txt
````

## Install IDE
PyCharm : https://www.jetbrains.com/pycharm/download/
ou VSCode

## Main Project

````
[options.entry_points]
console_scripts =
```` 
Reinstall package
````
pip install -U -e .
````

## Pycharm Debug
Use the file built by setup tools in the bin folder of your virtual ennv
Edit Configuration   
scriptPath: /path/to/bin/built_consoles_script_file  
arguments: def arguments 

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

## Dataframes
* inferSchema: spark will process input data and detect types
* writing dataframes modes: error(default), append, overwrite

## env vars
export SPARK_HOME=/path/to/spark  
export SPARK_DIST_CLASSPATH=$(hadoop classpath)  
export SPARK_DIST_CLASSPATH=/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/share/hadoop/yarn/*:/usr/local/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/contrib/capacity-scheduler/*.jar


