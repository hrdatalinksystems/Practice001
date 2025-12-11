# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "94d4b4e8-fd00-4d78-9d77-01c80f27684f",
# META       "default_lakehouse_name": "TestLaakehouse",
# META       "default_lakehouse_workspace_id": "71f0c899-a6f3-4cfb-8de8-a51b3f6e3e2a",
# META       "known_lakehouses": [
# META         {
# META           "id": "94d4b4e8-fd00-4d78-9d77-01c80f27684f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StringType, IntegerType, DataType, FloatType, StructField

orderSchema = StructType([
    StructField("SaalesOrderNumber",StringType()),
    StructField("")
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
