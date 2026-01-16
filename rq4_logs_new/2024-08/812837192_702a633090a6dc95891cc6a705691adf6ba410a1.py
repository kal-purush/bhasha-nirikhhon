import requests, os

def upload(user, repo, directory):
    upload_id = ""
    selectedFiles = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            selectedFiles.append(os.path.join(root, file))
    
    for index, file in enumerate(selectedFiles):
        retries = 0
        while retries < 3:
            try:
                presigned_url_params = {"file": file.name}
                if index != 0:
                    presigned_url_params["upload_id"] = upload_id
                
                presigned_url_response = requests.get(
                    f"https://t9wpxv65vi.execute-api.us-east-1.amazonaws.com/v0/{user}/{repo}/upload/s3",
                    params=presigned_url_params
                )
                
                presigned_url = presigned_url_response.json()["presigned_url"]
                if index == 0:
                    upload_id = presigned_url_response.json()["upload_id"]
                
                with open(file, "rb") as f:
                    response = requests.put(
                        presigned_url,
                        data=f,
                        headers={
                            "Content-Type": file.type
                        }
                    )
                
                setProgress((index + 1) / len(selectedFiles) * 100)
                setSelectedFiles(prevFiles => prevFiles.filter(f => f !== file))
                break
            except Exception as error:
                print(f"Error uploading file {file.name}: {error}")
                retries += 1

upload("seanmabli", "frc2024no2", "/home/synodic/datasets")