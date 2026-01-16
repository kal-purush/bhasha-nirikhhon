"""
Tests dask scheduler and workers responsiveness.
"""

import numpy as np
from dask.distributed import Client

# with Client(cluster) as client:
with Client(address="tcp://0.0.0.0:8786") as client:
    assert client.status == "running", f"client.status is `{client.status}`."
    print(repr(client))
    print("Connected :)")

    print("Sending dummy task ...")
    import dask.array as da

    arr = da.random.random((1000, 1000), chunks=(100, 100))
    assert np.isclose(arr.mean().compute(), 0.5, rtol=1e-1)
    print("Got expected result.")
    # 0.5001550986751964

    print("Result of submit: {}".format(client.submit(lambda x: x + 1, 10, key="test").result()))

    # Close the client
    print("Dask Sanity Completed Successfully.")