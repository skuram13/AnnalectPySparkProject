# About this Project

Dockerized spark cluster to analyze the Crude Oil dataset.
data set pulled into repo from https://www.kaggle.com/datasets/alistairking/u-s-crude-oil-imports/data

- spark_apps folder has the pyspark scripts for all the 3 questions
- Output folder has question 1 (What are the top 5 destinations for oil produced in Albania) stored in apache iceberg format
- results folder has output of all the 3 questions in csv format
- Docker volumes are set(persistent storage) to share data between container and local

# Make file
- Created make file which has commands to spin up docker containers and run pyspark jobs in spark cluster
```shell
eg:
make build
```

# Commands for running spark cluster
Run the spark standalone cluster by running below command:
```shell
make run
```
or with 3 workers using:
```shell
make run-scaled
```
Python job commands for first question in take home assignment:
Here it loads data in apache iceberg format
```shell
make submit-iceberg app=albania_top_five_destinations.py
```
Python job commands for 2nd and 3rd question in take home assignment:
```shell
make submit app=united_kingdom.py
make submit app=most_exported_grade.py
```
For this assignment running with 1 master and 1 worker node in spark cluster.
Just for the ease of understanding has seperated 3 questions into 3 apps(.py files), could be done in a single application as well!

## Web UIs
master node :
`localhost:9090`.

spark history server :
`localhost:18080`.

