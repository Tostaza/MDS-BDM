from dataclasses import dataclass
import io
import json
import os
from .client import Client
import boto3
import pandas as pd
from io import BytesIO, StringIO


@dataclass
class Seaweedfs(Client):
    
    s3_client: boto3.client = None

    def connect(self):
        self.aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_password = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.api_endpoint = os.environ.get("S3_ENDPOINT")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_password,
            endpoint_url=self.api_endpoint,
        )

    def get_object(self, Bucket, Key):
        if self.s3_client is None:
            self.connect()
        return self.s3_client.get_object(Bucket=Bucket, Key=Key)
    
    def upload_object(self, bucket, key, obj_bytes):
        if self.s3_client is None:
            self.connect()

        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=obj_bytes,
        )

    def upload_file(self, bucket, key, file_path):
        if self.s3_client is None:
            self.connect()
        self.s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=key
        )     

    def upload_dataframe(self, bucket, key, df):
        if self.s3_client is None:
            self.connect()

        buffer = io.StringIO()
        df.to_csv(buffer, index=False)    
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
        )

    def list_all_objects(self, Bucket:str = None, Prefix: str = None):
        if Bucket is None or Prefix is None:
            print("Missing Bucket or Prefix for SeaWeedFS client")
            return
        
        if self.s3_client is None:
            self.connect()

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=Bucket, Prefix=Prefix)
        object_keys = []
        for page in pages:
            for obj in page.get("Contents", []):
                object_keys.append(obj["Key"])

        if not object_keys:
            print("No new files found in the Landing Zone.")
            return

        all_data = []
        for key in object_keys:
            if key.endswith('.json'):
                file_obj = self.s3_client.get_object(Bucket=Bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    data = json.loads(file_content)
                    all_data.append(data)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {key}")
            elif key.endswith('.csv'):
                file_obj = self.s3_client.get_object(Bucket=Bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    df = pd.read_csv(StringIO(file_content))
                    all_data.extend(df.to_dict('records'))
                except Exception as e:
                    print(f"Error reading CSV from {key}: {e}")

        if not all_data:
            print("No valid JSON or CSV data found.")
            return None
        return all_data


# ---

    def load_xml(self, bucket, json_bytes, key):
        if self.s3_client is None:
            self.connect()
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_bytes,
        )
    
    def load_doc(self, bucket, df, key):
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)

        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        
    def load_json(self, bucket, doc, key):
        new_key = "/real-time"+key
        self.s3_client.put_object(
            Bucket=bucket,
            Key=new_key,
            Body=json.dumps(doc).encode("utf-8")
        )

    def download(self, bucket: str, key: str, file_path: str):
        if self.s3_client is None:
            self.connect()
        self.s3_client.download_file(bucket, key, file_path)

    def download_all(self, bucket: str):
        if self.s3_client is None:
            self.connect()
        all_objs = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                all_objs.append(obj)
        return all_objs
    
    def load_real_time(self, bucket, doc, key):
        self.create_bucket(bucket)
        self.load_json(bucket, doc, key)
        
        
    def create_bucket(self, name_bucket):
        try:
            self.s3_client.create_bucket(Bucket=name_bucket)
        except (self.s3_client.exceptions.BucketAlreadyExists, self.s3_client.exceptions.BucketAlreadyOwnedByYou):
            print(f"Bucket '{name_bucket}' already exists")
    
