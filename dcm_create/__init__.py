import json
import logging
import os
from datetime import date, timedelta

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlockBlobService, PublicAccess

from shared_code import dcm_class
from fields import dimensions, metric_names

# log into key vault and get key
KVUri = os.environ["KVUri"]
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

JSON_KEY = client.get_secret("google-service-account-json-file")
JSON_KEY_VALUE = eval(JSON_KEY.value)

start_date = date.today() - timedelta(days=1)
end_date = date.today() - timedelta(days=1)


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_json = req.get_json()
    logging
    account_id = req_json.get("account_id")

    dcm = dcm_class.dcmConnectorForAzure(account_id, JSON_KEY_VALUE)
    dcm.authenticate_using_service_account()
    dcm.create_and_run_report(start_date, end_date, metric_names, dimensions)

    return func.HttpResponse(json.dumps({"report_id": int(dcm.report_id), "upload_folder": "dcm"}))
