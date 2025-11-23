import s3fs
import time
import os

def init_minio():
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    print(f"Attempting to connect to MinIO at: {endpoint}")
    
    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': endpoint},
        key='admin', 
        secret='password'
    )
    
    # Retry logic for connection
    for i in range(10):
        try:
            fs.ls("") # Simple ping
            print("Connected to MinIO!")
            break
        except Exception as e:
            print(f"Waiting for MinIO... ({e})")
            time.sleep(2)
    
    try:
        if not fs.exists("warehouse"):
            fs.mkdir("warehouse")
            print("✅ Created bucket: warehouse")
        else:
            print("ℹ️ Bucket 'warehouse' already exists.")
    except Exception as e:
        print(f"❌ Critical Error creating bucket: {e}")
        exit(1)

if __name__ == "__main__":
    init_minio()

