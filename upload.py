from minio import Minio
from minio.error import S3Error

def get_minio_client():
    client = Minio(
        "localhost:9000",
        access_key = "minioadmin",
        secret_key = "minioadmin",
        secure = False
    )
    return client

def upload_file(bucket_name, object_name, file_path):
    try:
        client = get_minio_client()

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket 'mybucket' created.")
        else:
            print(f"Bucket 'mybucket' already exists.")


        client.fput_object(bucket_name, object_name, file_path)
        print(f"File 'test.csv' uploaded to bucket 'mybucket' as 'test.csv'.")
    except S3Error as e:
        print(f"Error uploading file: {e}")


# def download_file(bucket_name, object_name, file_path):
#     try:
#         client = get_minio_client()

#         client.fget_object(bucket_name, object_name, file_path)
#         print(f"File 'testcsv' downloaded from bucket 'mybucket' to 'test.csv'.")
#     except S3Error as e:
#         print(f"Error downloading file: {e}")

if __name__ == "__main__":
    bucket_name = "mybucket"
    upload_object_name = "test.csv"
    upload_file_path = "test.csv"

    upload_file(bucket_name, upload_object_name, upload_file_path)