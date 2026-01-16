import sys

import numpy as np

class Normalization_Layer:

    def normalize(self, data, n_method="batch_normalization"):
        print(n_method)
        if n_method == "batch_normalization":
            return self.__batch_normalization(data)
        sys.stdout.write("Error: The normalization method is not found\n")
        sys.exit(1)

    def __batch_normalization(self, data):
        # Batch Normalization
        data = (data - data.mean()) / (data.std() + 1e-9)
        data = data * (2 / (data.max() - data.min()))
        data -= (data.max() - 1)
        return data