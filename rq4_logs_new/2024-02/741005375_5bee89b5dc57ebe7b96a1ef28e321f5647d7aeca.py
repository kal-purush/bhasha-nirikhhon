import numpy as np

import torch
import numpy as np
import ot  # Python Optimal Transport library
import matplotlib.pyplot as plt


# Define your custom compute_distance function
def euclid_distance(x, y):
    # Implement your distance computation here
    # For example, Euclidean distance could be:
    return np.sqrt(np.sum((x - y) ** 2))




def compare_data_batches(data_real, data_generated = None, distance_func = None, outfile = None, numItermax=1000000, epsilon = 0.002, axis=None):
    # important: does not necessary return 1-to-1 mapping.

    distance_func = distance_func or euclid_distance

    # Convert PyTorch tensors to NumPy arrays if they are not already
    if isinstance(data_real, torch.Tensor):
        data_real = data_real.detach().cpu().numpy()
    if isinstance(data_generated, torch.Tensor):
        data_generated = data_generated.detach().cpu().numpy()
    assert data_real.shape[0] == data_generated.shape[0] # make sure num samples (batch_dim) is the same

    # 1) Compute the cost matrix between the two datasets
    # Using Euclidean distance here, but you can choose an appropriate measure
    # cost_matrix = ot.dist(data_real, data_generated, metric='euclidean')
    cost_matrix = np.zeros((len(data_real), len(data_generated)))

    # Fill in the cost matrix using your distance_func function
    for i in range(len(data_real)):
        for j in range(len(data_generated)):
            cost_matrix[i, j] = distance_func(torch.Tensor(data_real[i]), torch.Tensor(data_generated[j]))**2 #slow # TODO make scaling a parameter


    # Compute the optimal transport plan (coupling matrix) using the Sinkhorn algorithm
    # epsilon is the regularization term, you may need to adjust it
    #coupling_matrix = ot.sinkhorn(a=np.ones(len(data_real)) / len(data_real), b=np.ones(len(data_generated)) / len(data_generated), M=cost_matrix, reg=epsilon, numItermax=numItermax)
    coupling_matrix = ot.emd(a=np.ones(len(data_real)) / len(data_real), b=np.ones(len(data_generated)) / len(data_generated), M=cost_matrix, numItermax=numItermax)

    # Find the indices of the mappings (this works for small enough epsilon and assumes that data_real and data_generated have the same length)
    # For each point in data_real, find the index of its corresponding point in data_generated
    mapping_indices = np.argmax(coupling_matrix, axis=1)

    # Compute the overall transport cost
    overall_transport_cost = np.sum(coupling_matrix * cost_matrix)

    if outfile is not None or axis is not None:
        if axis is None:
            plt.figure(figsize=(8, 6))
            ax = plt.gca()
        else:
            ax = axis

        # Plot both datasets and the mapping
        ax.scatter(data_real[:, 0], data_real[:, 1], color='blue', label='Data real', alpha=0.5)
        ax.scatter(data_generated[:, 0], data_generated[:, 1], color='red', label='Data generated', alpha=0.5)

        # Draw lines between matched points
        for i, j in enumerate(mapping_indices):
            ax.plot([data_real[i, 0], data_generated[j, 0]], [data_real[i, 1], data_generated[j, 1]], color='gray', alpha=0.5)

        ax.set_xlabel('Dimension 1')
        ax.set_ylabel('Dimension 2')
        ax.set_title('Optimal Transport Map between Two Datasets')
        ax.legend()

        if outfile is not None:
            plt.savefig(outfile)

    return overall_transport_cost, mapping_indices



if __name__ == "__main__":
    # Set random seed for reproducibility
    np.random.seed(42)

    # Define batch size
    batch_size = 50

    # Create datasets data_0 and data_generated using 2D standard normal distribution
    data_0 = np.random.randn(batch_size, 2)
    data_generated = np.random.randn(batch_size, 2)

    compare_data_batches(data_0, data_generated, outfile="test_wasserstein.pdf")