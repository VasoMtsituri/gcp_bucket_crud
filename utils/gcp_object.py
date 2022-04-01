import logging

from google.auth.exceptions import DefaultCredentialsError

from utils.gcp_bucket import CloudStorageBucketClient

logging.basicConfig(level=logging.DEBUG)


class CloudStorageBucketObject:
    """
    Object level CRUD ops for Google Cloud Storage blobs
    """

    def __init__(self):
        try:
            self.__storage_client = CloudStorageBucketClient()
        except DefaultCredentialsError as auth_error:
            logging.debug(f'Authorization error: {auth_error}')

    @property
    def storage_client(self):
        return self.__storage_client

    def create_object(self, bucket_name, object_name, object_path):
        bucket = self.storage_client.retrieve_bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(object_path)
