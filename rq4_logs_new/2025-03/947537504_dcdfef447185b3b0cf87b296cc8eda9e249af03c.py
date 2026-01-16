# LIBRARIES / DEPENDENCIES
import os
import time
import requests

# URLs
source_url = "http://192.168.2.139/cgi-bin/status.xml"
destination_url = "https://coral-reef-capstone.vercel.app/api/xml"

# Maximum Backoff Time (10 minutes) to prevent excessive waiting
MAX_BACKOFF_TIME = 600
DATA_FOLDER = "/data"

# Ensure the data folder exists
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

time.sleep(3)

# Download the XML from the source URL and save it
def download_xml():
    backoff = 10  # Start with 10 seconds

    while True:
        try:
            print(f"Fetching text from {source_url}...")
            response = requests.get(source_url)
            
            # Check if the request was successful
            if response.status_code == 200:
                print("Download successful!")
                
                # Save data with a timestamp
                timestamp = int(time.time())  # Create unique filename
                file_path = os.path.join(DATA_FOLDER, f"data_{timestamp}.xml")
                
                with open(file_path, "w") as f:
                    f.write(response.text)  # The content of the XML
                
                print(f"Saved XML to {file_path}")
                return file_path
            
            else:
                print(f"Failed to download XML: {response.status_code}")
        
        except Exception as e:
            print(f"Error fetching XML: {e}")

        # Wait before retrying
        print(f"Retrying download in {backoff} seconds...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_TIME)  # Exponential backoff

# Upload XML files, ensuring all stored files get sent in case of an outage
def upload_pending_files():
    files = sorted(os.listdir(DATA_FOLDER))  # Sort to upload oldest first

    for file_name in files:
        file_path = os.path.join(DATA_FOLDER, file_name)

        with open(file_path, "r") as f:
            data = f.read()
        
        backoff = 10

        while True:
            try:
                print(f"Uploading {file_name} to {destination_url}...")
                headers = {'Content-Type': 'application/xml'}
                response = requests.post(destination_url, headers=headers, data=data)

                # Check if the request was successful
                if response.status_code == 200:
                    print(f"Upload successful: {file_name}")
                    os.remove(file_path)  # Delete file after successful upload
                    break  # Move to the next file
                else:
                    print(f"Upload failed. Status code: {response.status_code}")
            
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")
            
            # Wait before retrying
            print(f"Retrying upload of {file_name} in {backoff} seconds...")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_TIME)  # Exponential backoff

# MAIN LOOP
while True:
    # First, try uploading any stored data files BEFORE downloading new data
    upload_pending_files()

    # Download most recent data and store it
    download_xml()

    # Upload the most recent and downloaded data, which is now stored
    upload_pending_files()
    
    print("Sleeping for an hour...")
    time.sleep(3600)