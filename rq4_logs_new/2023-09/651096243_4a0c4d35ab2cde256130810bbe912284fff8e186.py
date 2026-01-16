
from itertools import islice

from torch.utils.data import IterDataPipe


class PatchesSampler(IterDataPipe):

    def __init__(self, datapipe, sampler, samples_per_volume):
        self.datapipe = datapipe
        self.sampler = sampler
        self.samples_per_volume = samples_per_volume

    def __iter__(self):
        for subject in self.datapipe:
            iterable = self.sampler(subject)
            yield list(islice(iterable, self.samples_per_volume))  # in my experience this turned out to be faster than using yield from islice(iterable, self.samples_per_volume)) and removing the UnBatcher