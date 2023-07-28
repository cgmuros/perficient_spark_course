# CREATE A DATAFRAME
df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")

# SCHEMA
df.printSchema()
spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema

# MANUAL SCHEMA
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True)
    , StructField("ORIGIN_COUNTRY_NAME", StringType(), True)
    , StructField("count", LongType(), False, metadata={"hello":"world"})
])

df = spark.read.format("json") \
    .schema(myManualSchema) \
    .load("data/flight-data/json/2015-summary.json")


# CREATING DATAFRAMES
df = spark.read.format("json").load("data/flight-data/json/2015-summary.json") 
df.createOrReplaceTempView("dfTable")
spark.sql("select * from dfTable").show()

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType 

myManualSchema = StructType([
    StructField("some", StringType(), True)
    , StructField("col", StringType(), True)
    , StructField("names", LongType(), False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema) 
myDf.show()

# select and selectExpr
from pyspark.sql.functions import expr, col, column

df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
df.select(expr("DEST_COUNTRY_NAME"), col("DEST_COUNTRY_NAME"), column("DEST_COUNTRY_NAME")).show(2)
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
df.selectExpr("*" ,"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# Literals
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

# Adding columns
df.withColumn("numberOne", lit(1)).show(2)

# Renaming columns
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

# Casting columns
df.withColumn("count2", col("count").cast("long"))

# Filtering
df.filter(col("count") < 2).show(2) 
df.where("count < 2").show(2)
df.where(col("count") < 2) \
    .where(col("ORIGIN_COUNTRY_NAME") != "Croatia") \
    .show(2)

# Union dataframes
from pyspark.sql import Row 
schema = df.schema
newRows = [Row("New Country", "Other Country", 5), Row("New Country 2", "Other Country 3", 1)]
parallelizedRows = spark.sparkContext.parallelize(newRows) 
newDF = spark.createDataFrame(parallelizedRows, schema)

df.union(newDF) \
    .where("count = 1") \
    .where(col("ORIGIN_COUNTRY_NAME") != "United States") \
    .show()

# Sorting
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5) 
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# Repartition (Full shuffle. greater than current number of partitions)
df.rdd.getNumPartitions()
new_df = df.repartition(5)
# new_df = df.repartition(col("DEST_COUNTRY_NAME")) # Repartition by column


df.write.option("header",True) \
        .partitionBy("DEST_COUNTRY_NAME") \
        .mode("overwrite") \
        .csv("tmp/test")

# Example Repartition and Coalesce
df_init = spark.range(0,20)
df_init = df_init.repartition(5)
print(df_init.rdd.getNumPartitions())
df_init.write.mode("overwrite").csv("tmp/init")

df_repartition = df_init.repartition(6)
print(df_repartition.rdd.getNumPartitions())
df_repartition.write.mode("overwrite").csv("tmp/repartition")

df_coalesce = df_init.coalesce(4)
print(df_coalesce.rdd.getNumPartitions())
df_coalesce.write.mode("overwrite").csv("tmp/coalesce")


# Join examples
# Sample data for the first DataFrame
data1 = [("A", 1), ("B", 2), ("C", 3)]
df1 = spark.createDataFrame(data1, ["key", "value"])

# Sample data for the second DataFrame
data2 = [("A", "X"), ("B", "Y"), ("D", "Z")]
df2 = spark.createDataFrame(data2, ["key", "info"])

# Perform a join between the DataFrames using the "key" column as the join key
df_result = df1.join(df2, on="key", how="inner")

# Show the result of the join
df_result.show()




# UDF examples
# Sample data for the DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Define the custom function for the UDF
def add_years(age):
    return age + 5

# Register the UDF with Spark
spark.udf.register("add_years_udf", add_years, IntegerType())

# Use the UDF in DataFrame operations
# Here, we create a new column "age_plus_5" using the UDF to add 5 to the "age" column
df = df.withColumn("age_plus_5", udf(lambda age: add_years(age), IntegerType())("age"))

# Show the resulting DataFrame
df.show()




# ArrayType Examples
# Sample data for the DataFrame
data = [
    ("Alice", 30, "HR", ["Python", "Communication"]),
    ("Bob", 25, "Engineering", ["Java", "Problem Solving", "SQL"]),
    ("Charlie", 35, "Finance", ["Data Analysis", "Excel"]),
    ("David", 28, "Engineering", ["C++", "Machine Learning"]),
]

# Define the schema for the DataFrame
schema = ["name", "age", "department", "skills"]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)


# Map Example
# Sample data for the DataFrame
data = [
    ("Alice", {"apple": 3, "banana": 2, "orange": 5}),
    ("Bob", {"grape": 4, "mango": 1}),
    ("Charlie", {"kiwi": 2, "pineapple": 3, "watermelon": 1}),
]

# Define the schema for the DataFrame
schema = ["name", "fruits"]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)


# Cache example
# Sample data for the DataFrame
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Cache the DataFrame
df.cache()

# Perform some computations on the cached DataFrame
# For example, count the number of rows
row_count = df.count()

# Show the DataFrame
df.show()

# Show the row count
print("Number of rows:", row_count)