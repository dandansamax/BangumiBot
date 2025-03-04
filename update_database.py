import os
import json
import shutil
import requests
from zipfile import ZipFile

def download_and_extract():
    json_path = "./bangumi_archive/aux/latest.json"
    extract_path = "./raw_data"
    
    # Ensure the JSON file exists
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found!")
        return
    
    # Load the JSON file
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    download_url = data.get("browser_download_url")
    file_name = data.get("name")
    
    if not download_url or not file_name:
        print("Error: Invalid JSON format, missing 'browser_download_url' or 'name'")
        return
    
    zip_path = os.path.join("./", file_name)
    
    # Check if existing raw_data is the newest version
    version_file = os.path.join(extract_path, "version.txt")
    if os.path.exists(version_file):
        with open(version_file, "r", encoding="utf-8") as vf:
            existing_version = vf.read().strip()
        if existing_version == file_name:
            print("Existing raw_data is up-to-date. No need to download.")
            return
    
    # Download the file
    print(f"Downloading {file_name} from {download_url}...")
    response = requests.get(download_url, stream=True)
    if response.status_code != 200:
        print(f"Error: Failed to download file, status code {response.status_code}")
        return
    
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Download complete.")
    
    # Remove old extracted folder if it exists
    if os.path.exists(extract_path):
        print("Removing old raw_data folder...")
        shutil.rmtree(extract_path)
    
    # Extract the zip file
    print(f"Extracting {file_name} to {extract_path}...")
    os.makedirs(extract_path, exist_ok=True)
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    print("Extraction complete.")
    
    # Save the version information
    with open(version_file, "w", encoding="utf-8") as vf:
        vf.write(file_name)
    
    # Cleanup downloaded zip file
    os.remove(zip_path)
    print("Cleanup complete.")

if __name__ == "__main__":
    download_and_extract()
