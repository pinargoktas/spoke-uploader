from google.oauth2 import service_account
from google.cloud import storage, bigquery
from datetime import date, datetime
import requests
import pandas as pd
import os
import json
import time


def main(event, context):
  #load credentials
  demsdata_project = '166318991862'
  credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ.get('DEMSDATA_SERVICE_ACCOUNT')))
  storage_client = storage.Client(project=demsdata_project, credentials=credentials)

  spoke_api_key = str(os.environ.get('SPOKE_API_KEY'))
  to_upload = pd.read_csv('https://docs.google.com/spreadsheets/d/1S0PFHyWXU2P4dSKH-hDJMYOgeWd_IVrZQJJyx4dQp94/gviz/tq?tqx=out:csv&sheet=Spoke').to_dict('records')
  to_upload = list(filter(lambda x: x['run_date'] in [str(date.today().strftime('%Y') + '-' + date.today().strftime('%m') + '-' + date.today().strftime('%d'))], to_upload))
#generates list_names if date matches today
  print(to_upload)
    # for each list:
  for i in to_upload:
    blobs = storage_client.list_blobs(bucket_or_name = 'bft_contact_lists', prefix='Lists_Texting/')
    blobs = [blob.name for blob in blobs]
    paths = [l for l in blobs if i['list_name'] in l]
    print(paths)

    copy_count = 0
    for path in paths:
    # clone campaign
      copy_count = copy_count + 1
      response = requests.request('POST',
                                      'https://betofortexas.text.scaletowin.com/api/copy-campaign',
                                      json={"campaignId": i['campaign_id'],
                                        "title": date.today().strftime("%m/%d") + " " + i['campaign_title']  + " Part " + str(copy_count) + "/" + str(len(paths))},
                                        headers={"STW-Api-Key": spoke_api_key})
      campaign_id = response.json()['campaignId']
      print(response.json())

    # upload list
      response = requests.request(
                  'POST',
                  'https://betofortexas.text.scaletowin.com/api/campaigns/{id}/contacts'.format(id=campaign_id),
                  json={"type": "csv-gcs-upload", "gcsBucket": 'bft_contact_lists', "gcsObjectName": path},
                  headers={"STW-Api-Key": spoke_api_key}
                ) #uploading files in gcs to the matched newly created campaigns
      print(response.json())
      time.sleep(60)  
