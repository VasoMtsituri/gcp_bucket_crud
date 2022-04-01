from utils.gcp_object import CloudStorageBucketObject

if __name__ == '__main__':
    s = CloudStorageBucketObject()
    s.retrieve_objects_and_download('some_shit_bucket', 'tmp')
