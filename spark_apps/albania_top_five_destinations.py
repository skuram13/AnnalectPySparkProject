import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Top five destinations for oil produced in Albania") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/spark/output/results/iceberg/") \
    .getOrCreate()

# Read the crude oil set CSV file into a DataFrame
file_path = "/opt/spark/apps/data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Create a temporary view of the DataFrame
df.createOrReplaceTempView("crude_oil_data")

# Run SQL query on the temporary view
# What are the top 5 destinations for oil produced in Albania?
query = "WITH crude_oil_sales AS (" \
        "SELECT sum(quantity) AS total_quantity_sold_per_destination , destinationName" \
        " FROM crude_oil_data " \
        " WHERE UPPER(originName) = 'ALBANIA'" \
        " and UPPER(originTypeName) = 'COUNTRY'" \
        " GROUP BY destinationName" \
        " ORDER BY total_quantity_sold_per_destination DESC)" \
        " select * from crude_oil_sales limit 5 "

# Execute the query and store the result in a new DataFrame
result_df = spark.sql(query)

# Show the result
result_df.show()


# saving output to a csv
result_df.write.mode("overwrite").option("header", "true").csv("/opt/spark/results/albania_top_five_destinations.csv")


# converting above results dataframe into Apache iceberg format.
# creating a apache iceberg table and loading above results into it

spark.sql("""
    CREATE or REPLACE TABLE local.crude_oil_schema.top_five_destinations_albania_tbl (
        total_quantity_sold_per_destination INT,
        destinationName STRING
    )
    USING iceberg
""")

# writing data into iceberg table created above
result_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("local.crude_oil_schema.top_five_destinations_albania_tbl")


# Read data from the created Iceberg table - to validate the load is success
result_iceberg = spark.read \
    .format("iceberg") \
    .load("local.crude_oil_schema.top_five_destinations_albania_tbl")

# Query the Iceberg table using SQL
result_iceberg_sql_way = spark.sql("SELECT * FROM local.crude_oil_schema.top_five_destinations_albania_tbl")

# Show the result
result_iceberg_sql_way.show()


## Note above app is run using make file command
## make submit-iceberg app=albania_top_five_destinations.py