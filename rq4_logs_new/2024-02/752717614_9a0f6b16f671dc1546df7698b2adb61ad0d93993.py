'''
Task 4

Download links.parquet
Download and save the first 10,000 images from the links in the parquet file.

- Monitor CPU and network usage during download.

References:
pyarrow package
requests package
'''
from multiprocessing import Pool
import os
import requests
import psutil
from pyarrow import parquet as pq
import time

def download_image(args):
    link, i = args
    print(i)
    # Check if image already exists
    if not os.path.exists(f'./data/image_{i}.jpg'):
        try:
            # Download image
            response = requests.get(link).content
            # Save image
            with open(f'./data/image_{i}.jpg', 'wb') as f:
                f.write(response)
        except Exception as e:
            pass

        print(f'Image {i} downloaded')
        print(f'CPU usage: {psutil.cpu_percent()}')
        print(f'Network usage: {psutil.net_io_counters()}')
        
            
def process_images(p_file, images_len):
    # Read the parquet file
    table = pq.read_table(p_file)
    # Get the links column
    links_list = table.column('URL').to_pylist()[:images_len]
    # Create a pool of workers
    p = Pool()
    # Create a mapping of the arguments
    args_list = [(link, i) for i, link in enumerate(links_list)]
    p.imap_unordered(download_image, args_list)
    p.close()  # Prevents any new tasks from being submitted
    p.join()   # Waits for all tasks to complete


if __name__ == "__main__":
    images_len = 10000
    # Path to parquet file
    parquet_path = './links.parquet'
    process_images(parquet_path,images_len)
