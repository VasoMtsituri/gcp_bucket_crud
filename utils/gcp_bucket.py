import os
import logging

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import DefaultCredentialsError

from utils.constants import GCP_CREDS, GCP_LOCATION_EU

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDS
logging.basicConfig(level=logging.DEBUG)


class CloudStorageBucketClient:
    """
    Client class used for connecting Google Cloud Storage and does bucket
    level operations

    """
    def __init__(self):
        try:
            self.__storage_client = storage.Client()
        except DefaultCredentialsError as auth_error:
            logging.debug(f'Authorization error: {auth_error}')

    @property
    def storage_client(self):
        return self.__storage_client

    def create_bucket(self, bucket_name, location=GCP_LOCATION_EU):
        """
        Creates the bucket in the Google cloud storage

        :param bucket_name: name of the bucket to be created
        :param location: physical location of servers in which
        this bucket must be
        :return: recently created Bucket object
        """
        try:
            self.storage_client.create_bucket(bucket_name, location=location)
            logging.debug(f'Bucket with name {bucket_name} created'
                          f' successfully')

            return 'OK', 201
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Creating new bucket failed'
        except ValueError as value_error:
            logging.debug(f'Invalid name: {value_error}')

            return 'Creating new bucket failed'

    def retrieve_bucket(self, bucket_name):
        """
        Returns the bucket with the given name

        :param bucket_name:  name of the bucket to be retrieved
        :return: Bucket object with the given name
        """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            logging.debug(f'Bucket with name {bucket_name} retrieved'
                          f' successfully')

            return bucket
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return f'Retrieving bucket with name {bucket_name} failed'
        except ValueError as value_error:
            logging.debug(f'Invalid name: {value_error}')

            return f'Retrieving bucket with name {bucket_name} failed'

    def retrieve_buckets(self, max_results=None, prefix=None):
        """
        Lists the buckets available in that particular project

        :param max_results: the number of buckets to be returned
        :param prefix: buckets with this prefix
        :return: all the buckets available in that particular project
        """
        try:
            buckets = self.storage_client.list_buckets(max_results=max_results,
                                                       prefix=prefix)
            logging.info(f'{len(buckets)} bucket(s) retrieved successfully')

            return buckets
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Retrieving buckets failed'

    def delete_bucket(self, bucket_name, force_deletion=False):
        """
        Deletes the bucket with the given name

        :param bucket_name: name of the bucket to be deleted
        :param force_deletion: flag whether to force deleting non-empty
        bucket or not
        :return: deleted status with code 204 if successful otherwise
        raises exception
        """
        try:
            bucket = self.retrieve_bucket(bucket_name)

            if type(bucket) == 'str':
                raise ValueError

            bucket.delete(force=force_deletion)
            logging.debug(f'Bucket with name {bucket_name} deleted'
                          f' successfully')

            return 'Deleted', 204
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return f'Deleting bucket with name {bucket_name} failed'
        except AttributeError:
            logging.debug(f'Invalid name or'
                          f' Bucket with that name does not exist')
