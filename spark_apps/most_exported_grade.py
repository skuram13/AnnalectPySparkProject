import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("most exported grade for each year and origin") \
    .getOrCreate()

# Read the crude oil set CSV file into a DataFrame
file_path = "/opt/spark/apps/data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("crude_oil_data")

# Run SQL query on the temporary view

query_final = "WITH CTE AS (SELECT year,originName, gradeName, sum(quantity) AS total_quantity " \
              " FROM crude_oil_data " \
              " GROUP BY year,originName, gradeName)," \
              "CTE2 AS (SELECT *, DENSE_RANK() OVER (PARTITION BY year, originName ORDER BY total_quantity " \
              "DESC) as " \
              "rnk from CTE)" \
              "SELECT * FROM CTE2 where rnk = 1 "

# Execute the query and store the result in a new DataFrame
result_df_final = spark.sql(query_final)

# Show the result
result_df_final.show()

# saving output to a csv
result_df_final.write.mode("overwrite").option("header", "true").csv("/opt/spark/results/most_exported_grade.csv")


## used command below to run this app
## make submit app=most_exported_grade.py