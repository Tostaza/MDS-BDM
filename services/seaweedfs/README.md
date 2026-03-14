# SeaweedFS Deployment

SeaweedFS is a simple, scalable, and highly available distributed file system. It incorporates an S3-compatible API, making it a drop-in replacement for S3 and other object storage systems in many applications.

## Docker compose
The docker compose file deploys a single container that executes all the SeaweedFS components (master, volume, and filer) in a single process using the [`weed mini`](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini) command. For production environments, go to the documentation to see which are the recommended methods.

Execute the following commands:

```bash
cp env.example .env
docker compose up -d
```

## List of available UIs
These UIs are available to the local machine through the browser:
- Master UI: http://localhost:9333
- Filer UI: http://localhost:8888
- S3 Endpoint: http://localhost:8333
- Admin UI: http://localhost:23646
- Volume Server: http://localhost:9340

To interact with the S3 API credentials are required (as defined in the env file)