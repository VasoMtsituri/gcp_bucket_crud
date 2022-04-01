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
        """
        Uploads object to the bucket given

        :param bucket_name: name of the bucket in which object be uploaded
        :param object_name: name of the object in the bucket (after upload)
        :param object_path: path of the object to be uploaded
        :return: status Created with code 201 if successful
        """

        bucket = self.storage_client.retrieve_bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(object_path)

        return 'Created', 201

    def retrieve_object(self, bucket_name, object_name, object_path):
        """
        Download the particular object from the given bucket

        :param bucket_name: name of the bucket in which object is located
        :param object_name: name of the object in the bucket
         to be downloaded
        :param object_path: path of the object (after download)
        :return: status Downloaded with code 200 if successful
        """

        bucket = self.storage_client.retrieve_bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.download_to_filename(object_path)

        return 'Downloaded', 200

    def retrieve_objects_as_blobs(self, bucket_name):
        """
        Gets all the objects from the given bucket as Blob objects

        :param bucket_name: name of the bucket in which objects are located
        :return: Blob objects
        """

        objects = self.storage_client.storage_client.list_blobs(bucket_name)

        objects = [blob for blob in objects]

        return objects

    def retrieve_objects_and_download(self, bucket_name, directory):
        """
        Download all the objects from the given bucket

        :param bucket_name: name of the bucket in which objects are located
        :param directory: path where all the objects will be downloaded
        :return: Blob objects
        """

        objects = self.storage_client.storage_client.list_blobs(bucket_name)

        objects = [blob.download_to_filename(f'{directory}/{blob.name}')
                   for blob in objects]
        logging.debug(f'{len(objects)} object(s) downloaded successfully')

        return 'Downloaded', 200

    def retrieve_object_names_only(self, bucket_name):
        """
        Lists all the objects names in the given bucket

        :param bucket_name:  name of the bucket in which objects are located
        :return: Blob objects' names
        """
        objects = self.retrieve_objects_as_blobs(bucket_name)
        names = [blob.name for blob in objects]

        return names

    def delete_object(self, bucket_name, object_name):
        """
        Deletes object from the given bucket

        :param bucket_name: name of the bucket in which object is located
        :param object_name: name of the object to be deleted
        :return: status Deleted with code 204 if successful
        """

        bucket = self.storage_client.retrieve_bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.delete()

        return 'Deleted', 204
