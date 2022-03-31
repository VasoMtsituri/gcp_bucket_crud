from utils.gcp_bucket import CloudStorageBucketClient

if __name__ == '__main__':
    client = CloudStorageBucketClient()

    client.create_bucket('-')

    print('Finished')
