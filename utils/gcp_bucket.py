import os
import logging
from urllib.error import HTTPError

from google.cloud import storage

from utils.constants import GCP_CREDS, GCP_LOCATION_EU

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDS
logging.basicConfig(level=logging.DEBUG)


class CloudStorageBucketClient:
    def __init__(self):
        self.__storage_client = storage.Client()

    @property
    def storage_client(self):
        return self.__storage_client

    def create_bucket(self, bucket_name, location=GCP_LOCATION_EU):
        try:
            self.storage_client.create_bucket(bucket_name, location=location)
            logging.debug('Bucket created successfully')
            return 'OK', 201
        except HTTPError as http_error:
            logging.debug(f'HTTPError error occurred: {http_error}')
            return 'Creating new bucket failed'
