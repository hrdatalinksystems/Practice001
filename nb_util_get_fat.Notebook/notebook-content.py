# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

def get_at():

    ti = ""
    ci = ""
    cs = ""

    url = ""
    headers = {
    }
    data = {
    }

    response = requests.post(url, headers=headers, data=data)
    response_json = response.json()
     
    return None



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
