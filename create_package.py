import os
import zipfile
import ibm_boto3
from ibm_botocore.client import Config
import base64
from config import get_config

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

# IBM Cloud / COS Config
BUCKET_NAME = get_config("BUCKET_NAME", required=True)
COS_ENDPOINT = get_config("COS_ENDPOINT", required=True)
COS_ACCESS_KEY = get_config("COS_ACCESS_KEY", required=True)
COS_SECRET_KEY = get_config("COS_SECRET_KEY", required=True)

# Derived paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def prepare_package():
    """Zips project files into dependencies.zip"""
    zip_name = os.path.join(BASE_DIR, "dependencies.zip")
    
    if os.path.exists(zip_name):
        os.remove(zip_name)

    print(f"Creating dependency package: {zip_name}...")
    with zipfile.ZipFile(zip_name, "w") as z:
        # Add core modules
        files_to_pack = ["dimension.py", "fact.py", "register_view.py", "config.py"]
        
        for f in files_to_pack:
            file_path = os.path.join(BASE_DIR, f)
            if os.path.exists(file_path):
                print(f"  Adding {f} to zip")
                z.write(file_path, arcname=f)
            else:
                print(f"  Warning: {f} not found, skipping.")
                
        # If user has other folders (e.g. netsuite_api_transform), add logic here
        # Example:
        # netsuite_dir = os.path.join(BASE_DIR, "netsuite_api_transform")
        # if os.path.exists(netsuite_dir):
        #      for root, dirs, files in os.walk(netsuite_dir):
        #         for file in files:
        #             z.write(os.path.join(root, file), 
        #                     arcname=os.path.relpath(os.path.join(root, file), BASE_DIR))

    return zip_name

def upload_to_cos(file_path, object_name=None):
    """Uploads a file to IBM COS"""
    if not object_name:
        object_name = os.path.basename(file_path)
        
    print(f"Uploading {file_path} to s3://{BUCKET_NAME}/{object_name}...")
    
    try:
        cos = ibm_boto3.client(
            "s3",
            aws_access_key_id=COS_ACCESS_KEY,
            aws_secret_access_key=COS_SECRET_KEY,
            endpoint_url=COS_ENDPOINT
        )
        cos.upload_file(file_path, BUCKET_NAME, object_name)
        print("Upload successful.")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")
        raise

if __name__ == "__main__":
    print("Starting Package Creation and Upload...")
    
    # 1. Prepare Zip
    zip_file = prepare_package()
    
    # 2. Upload Zip
    upload_to_cos(zip_file, "dependencies.zip")
    
    # 3. Upload main.py
    main_py_path = os.path.join(BASE_DIR, "main.py")
    if os.path.exists(main_py_path):
        upload_to_cos(main_py_path, "main.py")
    else:
        print("Error: main.py not found to upload!")
    
    print("Package creation and upload complete.")
