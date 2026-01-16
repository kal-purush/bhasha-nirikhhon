
import torch as th

from itertools import product
from math import pi as π

from torch import nn
from torch import optim
from torch.nn import Parameter
from torch.nn.modules import Module
from tqdm import tqdm, trange

from differentiable_dpp import DeterminantalPointProcess


def print(x):
    tqdm.write(str(x))


def Scalar(x):
    assert type(x) in {int, float}
    return th.Tensor([x])


class LEnsembleFactory(Module):
        """docstring for LEnsembleFactory"""
        def __init__(self, dim):
            super(LEnsembleFactory, self).__init__()
            self.dim = dim

            self.focalizer = nn.Sequential(
                nn.Linear(dim, dim),
                nn.Tanh(),
                nn.Linear(dim, 1),
                )

        def kernel(self, μ, μʹ, ρ):
            d = self.dim
            σ = 1

            term1 = (2 * ρ) ** (d / 2)
            term2 = (2 * π * σ**2) ** ((1 - 2*ρ) * d / 2)
            term3 = th.exp(- (ρ) * (th.norm(μ - μʹ) ** 2) / (4 * σ**2))
            return term1 * term2 * term3

        def focalization(self, μ):
            return th.exp(self.focalizer(μ))

        def make(self, μs, ρ=None):
            if not ρ:
                ρ = Scalar(0.01)
            N = len(μs)
            L = th.zeros(N, N)
            for ((i, μ), (j, μʹ)) in product(enumerate(μs), repeat=2):
                L[i, j] = self.kernel(μ, μʹ, ρ)
            for i, μ in enumerate(μs):
                L[i, i] += self.focalization(μ).squeeze()
            return L


class StepThree(Module):
    """probability of a subset and an alignment given a set"""
    def __init__(self, μs):
        super(StepThree, self).__init__()
        self.μs = μs  # Parameter(μs)
        self.μs_list = [μ for μ in μs]
        self.N = len(μs)

        self.L_factory = LEnsembleFactory(len(self.μs_list[0]))

    def log_prob(self, ordered_set):
        L = self.L_factory.make(self.μs_list)
        self.DPP = DeterminantalPointProcess(L)
        indices = []
        μs_list_list = [t.tolist() for t in self.μs_list]
        for elem in ordered_set:
            # print(elem)
            indices.append(μs_list_list.index(elem))
        # print(indices)
        indices.sort()
        indices = th.LongTensor(indices)
        return self.DPP(indices)

    def forward(self, ordered_set):
        return self.log_prob(ordered_set)


def make_data_and_samples():
    from numpy.random import poisson, seed
    import random

    random.seed(1337)
    seed(1337)

    length = 100
    size = 3
    data = [list(range(x, x + size)) for x in range(0, length * size, size)]

    n_samples = poisson(100)
    samples = []
    for i in range(n_samples):
        sample_size = poisson(20)
        sample = random.sample(data, k=sample_size)
        samples.append(sample)
    # print(data)
    # print(samples)
    return data, samples


def test_backprop():
    data, samples = make_data_and_samples()
    data = th.Tensor(data)
    s3 = StepThree(data)

    optimizer = optim.Adam(s3.parameters(), lr=0.01)
    for epoch in trange(5):
        total_loss = th.Tensor([0])

        for sample in tqdm(samples, leave=False):
            optimizer.zero_grad()
            log_prob = s3(sample)
            # print(f"\t{float(log_prob)}")
            loss = -log_prob
            total_loss += loss
            loss.backward()
            optimizer.step()

        print(float(total_loss.data) / len(samples))
        # print("X" * int(loss.data))


def main():
    # # data = [[x, x+1, x+2] for x in range(0, 300, 3)]
    # data, samples = make_data_and_samples()
    # print(len(data))
    # # sample = [data[4], data[3], data[1], data[15], data[9]]
    # data = th.Tensor(data)
    # s3 = StepThree(data)
    # # print(s3.N)
    # print(data[0].shape)
    # # sample = # [th.Tensor(t) for t in sample]
    # print(s3(samples[0]))

    test_backprop()

if __name__ == '__main__':
    main()