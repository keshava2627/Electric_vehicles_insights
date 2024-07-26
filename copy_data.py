from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/kesha/Desktop/Automative/keshanna-123-1fd09e8002cf.json'

def copy_sales_by_state(source_path, destination_bucket_name, destination_blob_name):
    client = storage.Client(project="keshanna-123")
    bucket = client.bucket(destination_bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_path)

    print(f"The file {source_path} was successfully uploaded to bucket {destination_bucket_name} with file name {destination_blob_name}.")
    
source_path = "C:/Users/kesha/Desktop/Automative/electric_vehicle_sales_by_state.csv"
destination_bucket = 'electric_automative'
destination_name = 'electric_vehicle_sales_by_state.csv'
copy_sales_by_state(source_path, destination_bucket, destination_name)
