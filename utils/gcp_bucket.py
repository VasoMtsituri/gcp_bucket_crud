import os
from google.cloud import storage
from utils.constants import GCP_CREDS

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDS


class CloudStorageBucketClient:
    def __init__(self):
        self.__storage_client = storage.Client()

    @property
    def storage_client(self):
        return self.__storage_client

    def create_bucket(self, bucket_name, location='eu'):
        self.storage_client.create_bucket(bucket_name, location=location)

