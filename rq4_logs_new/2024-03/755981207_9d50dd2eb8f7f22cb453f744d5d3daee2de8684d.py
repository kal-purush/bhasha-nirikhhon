import subprocess
import os

# Change directory to uploader API folder
uploader_api_folder = "/uploader"
os.chdir(uploader_api_folder)

# Start uploader API
uploader_api = subprocess.Popen(["python", "uploader_api.py"])

# Change directory to authenticate API folder
authenticate_api_folder = "/authenticate"
os.chdir(authenticate_api_folder)

# Start authenticate API
authenticate_api = subprocess.Popen(["python", "authenticate_api.py"])

# Change directory back to main directory
os.chdir("/main")

# Start main.py
main_api = subprocess.Popen(["python", "main.py"])

# Wait for APIs to finish
uploader_api.wait()
authenticate_api.wait()
main_api.wait()