import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UK destinations with total quantity greater than 100,000") \
    .getOrCreate()

# Read the crude oil set CSV file into a DataFrame
file_path = "/opt/spark/apps/data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("crude_oil_data")

# Run SQL query on the temporary view
query_uk = "SELECT sum(quantity) AS total_quantity , destinationName" \
           " FROM crude_oil_data " \
           " WHERE UPPER(originName) = 'UNITED KINGDOM'" \
           " and UPPER(originTypeName) = 'COUNTRY'" \
           " GROUP BY destinationName " \
           " HAVING sum(quantity) > 100000 "

# Execute the query and store the result in a new DataFrame
result_df_uk = spark.sql(query_uk)

# Show the result
result_df_uk.show()

# saving output to a csv
result_df_uk.write.mode("overwrite").option("header", "true").csv("/opt/spark/results/united_kingdom_results.csv")


## used command below to run this app
## make submit app=united_kingdom.py