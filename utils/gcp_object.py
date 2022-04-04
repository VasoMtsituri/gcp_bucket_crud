import logging

from google.api_core.exceptions import GoogleAPIError

from utils.gcp_bucket import CloudStorageBucketClient

logging.basicConfig(level=logging.DEBUG)


class CloudStorageBucketObject:
    """
    Object level CRUD ops for Google Cloud Storage blobs
    """

    def __init__(self):
        self.__storage_client = CloudStorageBucketClient()

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

        if type(bucket) != str:
            blob = bucket.blob(object_name)

            try:
                blob.upload_from_filename(object_path)

                return 'Created', 201
            except GoogleAPIError as google_api_error:
                logging.debug(f'GoogleAPIError occurred: {google_api_error}')

                return 'Uploading object to the bucket failed'
            except FileNotFoundError as not_found:
                logging.debug(not_found)

                return 'Uploading object to the bucket failed'
            except Exception as exception:
                logging.debug(f'Some other error occurred: {exception}')

                return 'Uploading object to the bucket failed'

    def retrieve_object(self, bucket_name, object_name, object_path):
        """
        Downloads the particular object from the given bucket

        :param bucket_name: name of the bucket in which object is located
        :param object_name: name of the object in the bucket
         to be downloaded
        :param object_path: path of the object (after download)
        :return: status Downloaded with code 200 if successful
        """

        bucket = self.storage_client.retrieve_bucket(bucket_name)

        if type(bucket) != str:
            blob = bucket.blob(object_name)

            try:
                blob.download_to_filename(object_path)

                return 'Downloaded', 200
            except GoogleAPIError as google_api_error:
                logging.debug(f'GoogleAPIError occurred: {google_api_error}')

                return f'Retrieving object with name {object_name} failed'
            except FileNotFoundError as not_found:
                logging.debug(not_found)

                return f'Retrieving object with name {object_name} failed'
            except Exception as exception:
                logging.debug(f'Some other error occurred: {exception}')

                return 'Retrieving object from the bucket failed'

    def retrieve_objects_as_blobs(self, bucket_name):
        """
        Gets all the objects from the given bucket as Blob objects

        :param bucket_name: name of the bucket in which objects are located
        :return: Blob objects
        """

        try:
            objects = self.storage_client.storage_client.list_blobs(
                bucket_name)
            objects = [blob for blob in objects]

            return objects
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Retrieving object from the bucket failed'
        except ValueError as value_error:
            logging.debug(f'Invalid name: {value_error}')

            return 'Retrieving object from the bucket failed'
        except Exception as exception:
            logging.debug(f'Some other error occurred: {exception}')

            return 'Retrieving object from the bucket failed'

    def retrieve_objects_and_download(self, bucket_name, directory):
        """
        Downloads all the objects from the given bucket

        :param bucket_name: name of the bucket in which objects are located
        :param directory: path where all the objects will be downloaded
        :return: Blob objects
        """

        try:
            objects = self.storage_client.storage_client.list_blobs(
                bucket_name)

            objects = [blob.download_to_filename(f'{directory}/{blob.name}')
                       for blob in objects]
            logging.debug(f'{len(objects)} object(s) downloaded successfully')

            return 'Downloaded', 200
        except GoogleAPIError as google_api_error:
            logging.debug(f'GoogleAPIError occurred: {google_api_error}')

            return 'Retrieving object from the bucket failed'
        except ValueError as value_error:
            logging.debug(f'Invalid name: {value_error}')

            return 'Retrieving object from the bucket failed'
        except FileNotFoundError as not_found:
            logging.debug(not_found)

            return 'Retrieving object from the bucket failed'
        except Exception as exception:
            logging.debug(f'Some other error occurred: {exception}')

            return 'Retrieving object from the bucket failed'

    def retrieve_object_names_only(self, bucket_name):
        """
        Lists all the objects names in the given bucket

        :param bucket_name:  name of the bucket in which objects are located
        :return: Blob objects' names
        """

        objects = self.retrieve_objects_as_blobs(bucket_name)

        if type(objects) != str:
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

        if type(bucket) != str:
            try:
                blob = bucket.blob(object_name)
                blob.delete()

                return 'Deleted', 204
            except GoogleAPIError as google_api_error:
                logging.debug(f'GoogleAPIError occurred: {google_api_error}')

                return f'Deleting object failed'
            except Exception as exception:
                logging.debug(f'Some other error occurred: {exception}')

                return f'Deleting object failed'
