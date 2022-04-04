from utils.gcp_object import CloudStorageBucketObject

if __name__ == '__main__':
    s = CloudStorageBucketObject()
    s.create_object('some_shit_ss', 'tmp.txt', '/home/vaso/PycharmProjects/gcp_bucket_crud/requirements.txt')
