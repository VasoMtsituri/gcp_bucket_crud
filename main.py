from utils.gcp_bucket import CloudStorageBucketClient

if __name__ == '__main__':
    client = CloudStorageBucketClient()

    name = 'someshit_bucket'

    client.create_bucket(name)

    print('Success')
