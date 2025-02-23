# About this Project

Dockerized spark cluster to Analyze the Crude Oil dataset.
data set in https://www.kaggle.com/datasets/alistairking/u-s-crude-oil-imports/data

- Docker volumes are set to share data between container and local


make file has commands to run pyspark jobs in spark cluster
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
Note : For this assignment running with 1 master and 1 worker node in spark cluster

## Web UIs
master node :
`localhost:9090`.

spark history server :
`localhost:18080`.

