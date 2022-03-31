# Imports the Google Cloud client library
import os
from google.cloud import storage
from constants import GCP_CREDS

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDS

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = 'bucket_from_crud'

# Creates the new bucket
bucket = storage_client.create_bucket(bucket_name)

print('Bucket {} created.'.format(bucket.name))
