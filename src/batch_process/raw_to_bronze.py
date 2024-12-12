from minio import Minio
from minio.error import S3Error
import os


# Raw data move to Bronze bucket
def get_minio_client():
    client = Minio(
        "localhost:9000",
        access_key = os.getenv("MINIO_USERNAME"),
        secret_key = os.getenv("MINIO_PASSWORD"),
        secure = False
    )
    return client

def upload_file(bucket_name, file_path):
    try:
        client = get_minio_client()

        if not client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating it now.")
            client.make_bucket(bucket_name)

        if not os.path.isfile(file_path):
            print(f"The file path '{file_path}' does not exist or is not a file.")
            return

        file_name = os.path.basename(file_path)

        
        client.fput_object(bucket_name, file_name, file_path)
        print(f"File '{file_name}' uploaded successfully to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"S3 Error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    data_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/data/Loan_default.csv"
    
    upload_file("bronze", data_path)
    