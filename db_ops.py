# #####uploaading datbase dat to cloud##########
# import boto3
# from pymongo import MongoClient
# from decimal import Decimal
#
# # Connect to DynamoDB
# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
# # table = dynamodb.Table('PtgCmsDevelopment')
# table = dynamodb.Table('PtgCmsProduction')
# # Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# # db = client['cmsDevelopment']
# db = client['cmsProduction']
#
#
# def convert_to_str(value):
#     if isinstance(value, dict):
#         return {keys: convert_to_str(values) for keys, values in value.items()}
#     if isinstance(value, (int, float, Decimal)):
#         return str(value)
#     if isinstance(value, (list, bool)):
#         return value
#     return value
#
#
# # Scan DynamoDB table and upload data to MongoDB
# response = table.scan()
# items = response['Items']
# print(len(items))
# count = 0
# for item in items:
#     item = {key: convert_to_str(value) for key, value in item.items()}
#     print(item['all_attributes'], type(item['all_attributes']))
#     collection = db[item["gsipk_table"]]
#     print(item["pk_id"], count)
#     # Insert the item into the selected collection
#     collection.insert_one(item)
#     count += 1
# print(count)
# print("Data upload complete.", count)

import boto3
from pymongo import MongoClient
from decimal import Decimal
# Define your AWS credentials
aws_access_key_id = "AKIA4F52A4XRXLIKFYOH"
aws_secret_access_key = "mtaL7S71M8Qh2TDgTTIHZDESHAbcOzK+G/A9tnaj"
region_name = "us-west-1"

# Connect to DynamoDB
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('PtgCmsProduction')

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['cmsProductionssssssss']


def convert_to_str(value):
    if isinstance(value, dict):
        return {keys: convert_to_str(values) for keys, values in value.items()}
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (list, bool)):
        print(type(value))
        return value
    return value
# Scan DynamoDB table and upload data to MongoDB
count = 0
start_key = None
while True:
    if start_key:
        response = table.scan(ExclusiveStartKey=start_key)
    else:
        response = table.scan()
    items = response['Items']
    print(len(items))
    for item in items:
        item = {key: convert_to_str(value) for key, value in item.items()}
        collection_name = item.get("gsipk_table")  # Get collection name dynamically
        collection = db[collection_name]
        print(item["pk_id"], count)
        collection.insert_one(item)  # Use the correct collection variable here
        count += 1
    # Check if there are more pages
    start_key = response.get('LastEvaluatedKey', None)
    if not start_key:
        break
print("Data upload complete.", count)





# import os

# import boto3

# s3 = boto3.client('s3')

# bucket_name = 'cms-image-data'

# def create_local_folder_path(file_key):

#     folders = file_key.split('/')

#     folders.pop()

#     local_folder_path = os.path.join('cms-image-data',*folders)

#     return local_folder_path

# def download_and_save_with_same_folder_structure(bucket_name, file_key):

#     local_folder_path = create_local_folder_path(file_key)

#     os.makedirs(local_folder_path, exist_ok=True)

#     local_file_name = os.path.basename(file_key)

#     local_path = os.path.join(local_folder_path, local_file_name)

#     s3.download_file(bucket_name, file_key, local_path)

#     print(f"Downloaded {file_key} to {local_path}")

# response = s3.list_objects_v2(Bucket=bucket_name)

# count=0

# for obj in response['Contents']:

#     file_key = obj['Key']

#     if "PtgCmsProduction"  in file_key  and "OPTG16_QAID01" not in  file_key and "OPTG20_QAID01" not in file_key:

#         download_and_save_with_same_folder_structure(bucket_name, file_key)

#         print(count)

#         count+=1

# print("Uploaded Successfully")


# def replace_url(data):

#     if isinstance(data, dict):

#         for key, value in data.items():

#             if isinstance(value, str):

#                 data[key] = value.replace(old_part, new_part)

#             elif isinstance(value, dict):

#                 replace_url(value)

#             elif isinstance(value, list):

#                 for item in value:

#                     replace_url(item)


# from pymongo import MongoClient

# client = MongoClient('mongodb://localhost:27017/')

# db = client['cmsProduction']
# # db.getCollectionNames()
# collections = db.list_collection_names()

# print(collections)
# # print(list(db.listCollections()))

# # table=[]
# for i in collections:

#     collection = db[i]

#     old_part = 'https://cms-image-data.s3.us-west-1.amazonaws.com'

#     new_part = 'cms-image-data'

#     for document in collection.find():

#         print(document)

#         replace_url(document)

#         collection.update_one({'_id': document['_id']}, {'$set': document})

# client.close()

















# ######uploaading datbase dat to cloud##########
# import boto3
# from pymongo import MongoClient
# from decimal import Decimal
# # Connect to DynamoDB
# dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
# # table = dynamodb.Table('PtgCmsDevelopment')
# table = dynamodb.Table('PtgCmsProduction')
# # Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# # db = client['cmsDevelopment']
# db = client['cmsProduction']
# def convert_to_str(value):
#     if isinstance(value,dict):
#         return {keys:convert_to_str(values) for keys,values in value.items()}
#     if isinstance(value, (int, float, Decimal)):
#         return str(value)
#     return str(value)
# # Scan DynamoDB table and upload data to MongoDB
# response = table.scan()
# items = response['Items']
# print(len(items))
# count=0
# for item in items:
#     item={key: convert_to_str(value) for key, value in item.items()}
#     print(item['all_attributes'],type(item['all_attributes']))
#     collection = db[item["gsipk_table"]]
#     print(item["pk_id"],count)
#     # Insert the item into the selected collection
#     collection.insert_one(item)
#     count+=1
# print(count)
# print("Data upload complete.",count)
import os
#
# import boto3
#
# s3 = boto3.client('s3')
#
# bucket_name = 'cms-image-data'
#
# def create_local_folder_path(file_key):
#
#     folders = file_key.split('/')
#
#     folders.pop()
#
#     local_folder_path = os.path.join('cms-image-data',*folders)
#
#     return local_folder_path
#
# def download_and_save_with_same_folder_structure(bucket_name, file_key):
#
#     local_folder_path = create_local_folder_path(file_key)
#
#     os.makedirs(local_folder_path, exist_ok=True)
#
#     local_file_name = os.path.basename(file_key)
#
#     local_path = os.path.join(local_folder_path, local_file_name)
#
#     s3.download_file(bucket_name, file_key, local_path)
#
#     print(f"Downloaded {file_key} to {local_path}")
#
# response = s3.list_objects_v2(Bucket=bucket_name)
#
# count=0
#
# for obj in response['Contents']:
#
#     file_key = obj['Key']
#
#     if "PtgCmsProduction"  in file_key  and "OPTG16_QAID01" not in  file_key and "OPTG20_QAID01" not in file_key:
#
#         download_and_save_with_same_folder_structure(bucket_name, file_key)
#
#         print(count)
#
#         count+=1
#
# print("Uploaded Successfully")
#
#
# def replace_url(data):
#
#     if isinstance(data, dict):
#
#         for key, value in data.items():
#
#             if isinstance(value, str):
#
#                 data[key] = value.replace(old_part, new_part)
#
#             elif isinstance(value, dict):
#
#                 replace_url(value)
#
#             elif isinstance(value, list):
#
#                 for item in value:
#
#                     replace_url(item)
#
#
# from pymongo import MongoClient
#
# client = MongoClient('mongodb://localhost:27017/')
#
# db = client['cmsProduction']
# # db.getCollectionNames()
# collections = db.list_collection_names()
#
# print(collections)
# # print(list(db.listCollections()))
#
# # table=[]
# for i in collections:
#
#     collection = db[i]
#
#     old_part = 'https://cms-image-data.s3.us-west-1.amazonaws.com'
#
#     new_part = 'cms-image-data'
#
#     for document in collection.find():
#
#         print(document)
#
#         replace_url(document)
#
#         collection.update_one({'_id': document['_id']}, {'$set': document})
#
# client.close()

#
#
# import boto3
#
# # Replace 'YOUR_ACCESS_KEY' and 'YOUR_SECRET_KEY' with your actual AWS credentials
# ACCESS_KEY = "AKIA4F52A4XRXLIKFYOH"
# SECRET_KEY = "mtaL7S71M8Qh2TDgTTIHZDESHAbcOzK+G/A9tnaj"
#
# # Replace 'your-bucket-name' with your S3 bucket name
# BUCKET_NAME = 'cms-image-data'
#
# # Replace 'path/to/your/image.jpg' with the path to your image file
# FILE_PATH = 'D:\\CMS_Backend\\CMS_Backend\\cms-image-data\\PtgCmsProduction\\categoryimage\\Electronic\\00045\\00045.jpg'
# REGION_NAME="us-west-1"
# def upload_to_s3(file_path, bucket_name):
#     # Create an S3 client
#     s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION_NAME)
#
#     # Upload the file
#     try:
#         s3.upload_file(file_path, bucket_name, file_path.split('/')[-1])
#         print("Upload Successful")
#     except Exception as e:
#         print("Upload Failed:", e)
#
# # Call the function to upload the image
# upload_to_s3(FILE_PATH, BUCKET_NAME)





