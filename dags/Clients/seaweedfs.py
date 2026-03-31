from dataclasses import dataclass
import io
import json
import os
from .client import Client
import boto3
from io import BytesIO, StringIO


@dataclass
class Seaweedfs(Client):
    
    s3_client: Client = None
    def connect(self):
        self.aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_password = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.api_endpoint = os.environ.get("SEAWEEDFS_S3_ENDPOINT")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_password,
            endpoint_url=f"http://seaweedfs:8333",
        )
    
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
    
