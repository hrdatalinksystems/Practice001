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


def get_access_token():

    tenant_id = ""
    client_id = ""
    client_secret = ""

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://analysis.windows.net/powerbi/api/.default'
    }

    response = requests.post(url, headers=headers, data=data)
    response_json = response.json()
    access_token = response_json.get('access_token')
    
    return access_token


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
