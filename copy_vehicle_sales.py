from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:/Users/kesha/Desktop/Automative/keshanna-123-1fd09e8002cf.json'

def copy_sales_by_makers(source_path,destination_bucket,destination_blob_name):
    client=storage.Client(project="keshanna-123")
    bucket=client.get_bucket(destination_bucket)
    blob=storage.Blob(destination_blob_name,bucket=bucket)
    blob.upload_from_filename(source_path)
    print(f'the file {source_path} is sucessfully uploaded to {destination_bucket} with file name {destination_blob_name}.')


source_path="C:/Users/kesha/Desktop/Automative/electric_vehicle_sales_by_makers.csv"
destination_bucket="electric_automative"
destination_blob_name="electric_vehicle_sales_by_makers.csv"

copy_sales_by_makers(source_path,destination_bucket,destination_blob_name)