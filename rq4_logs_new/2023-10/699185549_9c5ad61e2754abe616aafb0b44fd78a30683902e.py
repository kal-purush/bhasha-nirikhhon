from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

def authenticate_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and handles user authorization.
    drive = GoogleDrive(gauth)
    return drive

def upload_file_to_drive(drive, local_file_path, drive_folder_id):
    file_name = os.path.basename(local_file_path)
    gfile = drive.CreateFile({'title': file_name, 'parents': [{'id': drive_folder_id}]})
    gfile.Upload()
    print(f"File '{file_name}' uploaded to Google Drive.")

def backup_to_google_drive(local_folder_path, drive_folder_id):
    drive = authenticate_drive()

    # Walk through the local folder and upload files to Google Drive
    for foldername, subfolders, filenames in os.walk(local_folder_path):
        for filename in filenames:
            local_file_path = os.path.join(foldername, filename)
            upload_file_to_drive(drive, local_file_path, drive_folder_id)

if __name__ == "__main__":
    # Replace these values with your own
    local_project_folder = "1.1"
    google_drive_folder_id = "1RpbnVk_PHF2OZAmie8i4t9wFdEBePJwA"

    backup_to_google_drive(local_project_folder, google_drive_folder_id)