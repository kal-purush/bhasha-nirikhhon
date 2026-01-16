import dask
import do
from dask.distributed import Client
from os import listdir
from os.path import join

if __name__ == "__main__":
    input_directory = "D:/Documents/MISSIONS/DASK/pdal/input"
    args = {
        'output_dir': "D:/Documents/MISSIONS/DASK/pdal/output"
    }

    files = [join(input_directory, f) for f in listdir(input_directory)]

    delayed = do.processPipelines(files, args)
    client = Client(n_workers=5, threads_per_worker=1)
    dask.compute(*delayed)