from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/kesha/Desktop/Automative/keshanna-123-1fd09e8002cf.json"


def bucket_create(bucket_id):
    client=storage.Client(project="keshanna-123")
    bucket=client.bucket(bucket_id)

    try:
        bucket=client.get_bucket(bucket_id)
        print(f"the bucket {bucket_id} is already exists.")
    except NotFound:
        bucket=client.create_bucket(bucket_id)
        print(f"the bucket {bucket_id} is created successfully.")

bucket_id="electric_automative"
bucket_create(bucket_id)