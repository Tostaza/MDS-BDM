from dataclasses import dataclass
import json
import os
from dotenv import load_dotenv
from .client import Client
import boto3


@dataclass
class Seaweedfs(Client):
    
    s3_client: Client = None
    
    def connect(self):
        
        load_dotenv()
        self.aws_key = os.getenv("ACCESS_KEY_ID")
        self.aws_password = os.getenv("SECRET_ACCESS_KEY")
        self.api_endpoint = os.getenv("API_ENDPOINT")
        if not self.aws_key or not self.aws_password or not self.api_endpoint:
            self.connect()
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_password,
            endpoint_url="http://" + self.api_endpoint,
        )
    
    def load(self, bucket, json_bytes):
        self.s3_client.put_object(
            Bucket=bucket,
            Key="barcelona_data_3.json",
            Body=json_bytes,
        )

    def download_all(self, bucket: str):
        if self.s3_client is None:
            self.connect()
        all_objs = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                all_objs.append(obj)
        return all_objs
        
    def create_bucket(self, name_bucket):
        try:
            self.s3_client.create_bucket(Bucket=name_bucket)
        except (self.s3_client.exceptions.BucketAlreadyExists, self.s3_client.exceptions.BucketAlreadyOwnedByYou):
            print(f"Bucket '{name_bucket}' already exists")
    
