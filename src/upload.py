from minio import Minio
from minio.error import S3Error
import os



def get_minio_client():
    client = Minio(
        "localhost:9000",
        access_key = os.getenv("MINIO_USERNAME"),
        secret_key = os.getenv("MINIO_PASSWORD"),
        secure = False
    )
    return client

def upload_file(bucket_name, object_name, file_path):
    try:
        client = get_minio_client()

        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        if os.path.isfile(file_path):
            client.fput_object(bucket_name, object_name, file_path)
            print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
        else:
            print(f"Error: '{file_path}' is not a file.")
    except Exception as e:
        print(f"An error occurred: {e}")


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
    upload_file_path = "data/test.csv"

    upload_file(bucket_name, upload_object_name, upload_file_path)