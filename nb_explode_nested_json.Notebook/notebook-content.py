# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0dd49c66-81f8-40cf-a8a6-d612788d3a6d",
# META       "default_lakehouse_name": "TestLakehouse002",
# META       "default_lakehouse_workspace_id": "71f0c899-a6f3-4cfb-8de8-a51b3f6e3e2a",
# META       "known_lakehouses": [
# META         {
# META           "id": "0dd49c66-81f8-40cf-a8a6-d612788d3a6d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType,
    ArrayType
)

from pyspark.sql.functions import col, explode

user_schema = StructType([
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("address", StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("zip", IntegerType())
    ]))
])

item_schema = StructType([
    StructField("sku", StringType()),
    StructField("qty", IntegerType()),
    StructField("price", FloatType())
])

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("items", ArrayType(item_schema))
])

full_schema = StructType([
    StructField("id", IntegerType()),
    StructField("user", user_schema),
    StructField("orders", ArrayType(order_schema))
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("json").option("multiline", "true").schema(full_schema).load("Files/nested_json_sample.json")
#df.printSchema()
df = df.select(
    "id",
    col("user.name").alias("customer_name"),
    col("user.address.city").alias("city"),
    explode("orders").alias("order")
    )
display(df)
df = df.select(
    "id",
    "customer_name",
    "city",
    col("order.order_id").alias("order_id"),
    explode("order.items").alias("order_item")
    )
display(df)
df = df.select(
    "id",
    "customer_name",
    "city",
    "order_id",
    col("order_item.sku").alias("order_item_sku"),
    col("order_item.qty").alias("order_item_qty"),
    col("order_item.price").alias("order_item_price")
    )
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
