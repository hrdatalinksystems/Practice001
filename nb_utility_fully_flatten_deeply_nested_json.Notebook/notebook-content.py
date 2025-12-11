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

from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def flatten_df(df):
    """
    Recursively flattens a nested DataFrame with structs and arrays.
    Arrays of structs will be exploded using explode_outer.
    """
    
    def _flatten(schema, prefix=""):
        fields = []
        for field in schema.fields:
            name = f"{prefix}.{field.name}" if prefix else field.name
            fields.append((name, field.dataType))
        return fields

    flat_cols = []
    explode_cols = []

    for name, dtype in _flatten(df.schema):
        if isinstance(dtype, StructType):
            # Struct: keep expanding
            for f in dtype.fields:
                col_name = f"{name}.{f.name}"
                #temp_df = df.select(col(col_name))
                #display(temp_df)
                #for tname, tdtype in _flatten(temp_df.schema): 
                #    print("Column Name:", col_name, "Data Type:", tdtype)
                flat_cols.append(col(col_name).alias(col_name.replace(".", "_")))
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            # Array of struct: explode it
            explode_cols.append(name)
        else:
            # Simple column
            flat_cols.append(col(name).alias(name.replace(".", "_")))
        print(flat_cols)
        print(explode_cols)

    # If there is an array of structs, explode the first one and recurse
    if explode_cols:
        col_to_explode = explode_cols[0]
        df = df.withColumn(col_to_explode, explode_outer(col_to_explode))
        df.printSchema()
        return flatten_df(df)

    return df.select(flat_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("json").option("multiline", "true").load("Files/nested_json_sample.json")
#df.printSchema()
df = flatten_df(df)
df.printSchema()
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
