import numpy as np
import time

def convert_array_to_bytes(arr):
    """Convert a numpy array to bytes."""
    return arr.tobytes()

def convert_bytes_to_array(byte_array, shape, dtype):
    """Convert bytes back to a numpy array."""
    return np.frombuffer(byte_array, dtype=dtype).reshape(shape)


if __name__ == "__main__":
    arr_shape = (1000, 1000)
    arr_dtype = np.float32

    # Create a random numpy array
    arr = np.random.rand(*arr_shape).astype(arr_dtype)

    # Convert the numpy array to bytes
    arr_bytes = convert_array_to_bytes(arr)

    # Convert the bytes back to a numpy array
    arr_reconstructed = convert_bytes_to_array(arr_bytes, arr_shape, arr_dtype)

    # Check if the reconstructed array is the same as the original
    print(np.allclose(arr, arr_reconstructed))