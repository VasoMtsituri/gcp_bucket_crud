import os
import logging

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import DefaultCredentialsError

from utils.constants import GCP_CREDS, GCP_LOCATION_EU

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDS
logging.basicConfig(level=logging.DEBUG)


class CloudStorageBucketClient:
    def __init__(self):
        try:
            self.__storage_client = storage.Client()
        except DefaultCredentialsError as auth_error:
            logging.debug(f'Authorization error: {auth_error}')

    @property
    def storage_client(self):
        return self.__storage_client

    def create_bucket(self, bucket_name, location=GCP_LOCATION_EU):
        try:
            self.storage_client.create_bucket(bucket_name, location=location)
            logging.info('Bucket created successfully')

            return 'OK', 201
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Creating new bucket failed'
        except ValueError as value_error:
            logging.debug(f'Invalid name: {value_error}')

            return 'Creating new bucket failed'

    def retrieve_bucket(self, bucket_name):
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            logging.info('Bucket retrieved successfully')

            return bucket
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return f'Retrieving bucket with name {bucket_name} failed'

    def retrieve_buckets(self, max_results=None, prefix=None):
        try:
            buckets = self.storage_client.list_buckets(max_results=max_results,
                                                       prefix=prefix)
            logging.info('Buckets retrieved successfully')

            return buckets
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Retrieving buckets failed'

    def delete_bucket(self, bucket_name, force_deletion=False):
        try:
            bucket = self.retrieve_bucket(bucket_name)
            bucket.delete(force=force_deletion)

            return 'Deleted', 204
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return f'Deleting bucket with name {bucket_name} failed'
