from __future__ import division
import numpy as np
import yaml
from collections import defaultdict

def ends_at(split):
    return split[1]
def starts_at(split):
    return split[0]

class ViterbiDecompounder:

    def __init__(self):
        pass

    def load_weights(self, weight_file):
        self.w = np.array(yaml.load(open(weight_file, 'r').read()))

    def viterbi_decode(self, compound):

        print "\n"*4
        print compound.string

        alphas = [{} for _ in range(len(compound.string)+1)]
        path = { (0,0): [] }
        alphas[0] = defaultdict(lambda: 1.0)

        START_SPLIT = (0, 0)
        END_SPLIT = (len(compound.string),len(compound.string))

        lattice = compound.predicted_lattice
        print "Lattice:", str(lattice.lattice)
        print "Splits in the lattice:" + str(lattice.get_splits())

        for split in lattice.splits_from(0):
            path[split] = [(0,0)]
            print "From 0:", str(split)

        for i in range(len(compound.string)+1):
            new_path = path

            for split in lattice.splits_from(i):
                if len(lattice.splits_to(i)) > 0 and len(lattice.splits_from(i)) > 0:
                    (alphas[ split[1] ][split], b) = max([(alphas[ends_at(split_last)][split_last] + self.arc_score(compound, split_last, split, lattice), split_last) for split_last in lattice.splits_to(i)])

                    new_path[split] = path[b] + [split]

            path = new_path

        print "path",  path

        print "Final alphas"
        for i, a in enumerate(alphas):
            print i, a

        f = len(compound.string)
        (_, b) = max([(alphas[split_last[1]][split_last], split_last) for split_last in lattice.splits_to(f)])

        return path[ b ]

    def arc_score(self, compound, prev_split, split, lattice):
        return np.dot(self.w, self.fs(compound, prev_split, split, lattice))

    def fs(self, compound, prev_split, split, lattice):
        # Base features on the lattice:
        # (0, 1.0, 0, 1) rank, cosine, split penalty, is_no_split

        # Additional features:

        base_features = list(lattice.get_features(split, compound))
        base_features[0] = base_features[0] / 100

        base_features.append(1 if split[1] - split[0] == len(compound.string) else 0)  # Length of the split
        base_features.append(0 if split[1] - split[0] == len(compound.string) else 1)  # Length of the split

        #base_features.append(split[1] - split[0])  # Length of the split
        #base_features.append(prev_split[1] - prev_split[0])  # Length of the previous split
        base_features.append(1.0)  # Bias

        return np.array(base_features)

