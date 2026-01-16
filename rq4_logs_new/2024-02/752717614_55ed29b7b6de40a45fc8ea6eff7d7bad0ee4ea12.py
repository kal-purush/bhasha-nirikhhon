'''
Task 5

Create a class Downloader with the following template:

class Downloader:
    ...
    def __init__(self, pq_file: str):
        ...

where pq_file is the path to the parquet file.

At the end of the task, the following things should work on an instance of Downloader called d:

d[i] will download the i'th image and return its local path
d[i : j] will download i to j images and return their local paths in a list
'''
from pyarrow import parquet as pq
import os
import requests
from urllib.parse import urlparse

class Downloader:
    def __init__(self, pq_file, download_path = "./data"):
        self.pq_file = pq_file
        self.table = pq.read_table(pq_file)
        self.links_list = self.table.column('URL').to_pylist()
        self.download_path = download_path
        if not os.path.exists(download_path):
            os.mkdir(download_path)

    def download_image(self,i):
        # Retrieve the link
        link  = self.links_list[i]
        # Create the image path in the download dir
        image_path = os.path.join(self.download_path,f'image_{i}')
        if not os.path.exists(image_path):
            try:
                # Extract file extension from the URL
                parsed_url = urlparse(link)
                _, extension = os.path.splitext(parsed_url.path)
                # Download image
                response = requests.get(link)
                if extension:
                    # Save image with appropriate extension
                    with open(f'./data/image_{i}{extension}', 'wb') as f:
                        f.write(response.content)
                # If extension detection fails, save as .jpg
                with open(f'./data/image_{i}.jpg', 'wb') as f:
                    f.write(response.content)
                image_path = os.path.join(self.download_path,f'image_{i}{extension}')
            except Exception as e:
                print("Error: ",e)
                print(f"{i} not retrievable")
                return None
        return image_path

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.download_image(key)
        elif isinstance(key, slice):
            return [self.download_image(i) for i in range(key.start, key.stop)]
    
# Testing
        
if __name__ == "__main__":
    pq_file = '../task_4/links.parquet'
    d = Downloader(pq_file)
    print(d[2])
    print(d[1002])
    print(d[10050:10070])