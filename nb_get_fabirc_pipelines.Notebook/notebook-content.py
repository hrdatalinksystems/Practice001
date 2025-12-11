# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run nb_util_get_access_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_access_token()
print("Access Token->", access_token)
workspaceId = '71f0c899-a6f3-4cfb-8de8-a51b3f6e3e2a'
url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/dataPipelines"
headers = {
    'Authorization': f'Bearer {access_token}'
}
try:
    response = requests.get(url, headers=headers)
    print(response)
    response.raise_for_status()  # Raise error for bad responses (4xx or 5xx)
    response_json = response.json()
    response_array = response_json["value"]
    df = spark.createDataFrame(response_array)
    display(df)
    #print("Response json array->",response_array)
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
