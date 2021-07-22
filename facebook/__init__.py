import datetime
import json
import os
from datetime import date, timedelta

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from .fb_class import fbConnectorForAzure

# get secrets from azure key vault
KVUri = os.environ["KVUri"]
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

# for facebook access
access_token = client.get_secret("facebook-access-token").value
app_id = client.get_secret("facebook-app-id").value
app_secret = client.get_secret("facebook-app-secret").value

# for azure blob access - this is the blob account url
account_url = client.get_secret("blob-account-url").value

# dates to query
dates_to_query = [date.today() - timedelta(days=2),
                  date.today() - timedelta(days=1)]


def main(req: func.HttpRequest):
    req_body = req.get_json()

    if "act_" in req_body.get("ad_account"):
        ad_account = req_body.get("ad_account")
    else:
        ad_account = "act_" + req_body.get("ad_account")

    file_path = "/new/fb/"

    for i in dates_to_query:
        fb = fbConnectorForAzure(i, ad_account, access_token, app_id, app_secret,
                                 account_url, "bronze", file_path, i)
        fb.run()

    return func.HttpResponse(json.dumps({"status": "success"}))
