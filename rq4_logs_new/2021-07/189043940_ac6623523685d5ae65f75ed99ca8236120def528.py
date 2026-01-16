"""Module to check collinear safety of clustering algorithms """
import awkward as ak
import numpy as np
from jet_tools import GhostParticles
from jet_tools import FormJets


def collenar_splits(energies, pxs, pys, pzs, labels):
    split_fractions = np.random.rand(len(pxs))
    four_momentum = np.vstack((energies, pxs, pys, pzs))
    four_momentum1 = four_momentum*split_fractions
    four_momentum2 = four_momentum*(1-split_fractions)
    _, ints1, floats1 = GhostParticles.create_jet_internals(*four_momentum1,
                                                            labels=labels)
    labels2 = -(np.array(labels) + 2)
    _, ints2, floats2 = GhostParticles.create_jet_internals(*four_momentum2,
                                                            labels=labels2)
    return ints1, floats1, ints2, floats2


def clustering_with_collinear(clustering_algorithm, num_splits=None):
    """ Alter the clustering algorithm so that the init
    method makes collinear splits. It can then be used as before.
    The other particle in each pair has the label -(label+2)

    Parameters
    ----------
    clustering_algorithm : class inheriting from FormJets.Agglomerative
        the clustering algorithm to be modified.
    num_splits : int or None
        number of particles to be split,
        if not given they are chosent at random.

    Returns
    -------
    clustering_algorithm : class inheriting from FormJets.Agglomerative
        the modified clustering algorithm
    
    """
    if num_splits is None:
        def get_splits(num_particles):
            n_splits = np.random.choice(num_particles)+1
            splits = np.random.choice(num_particles, n_splits, replace=False)
            return splits
    else:
        def get_splits(num_particles):
            n_splits = min(num_splits, num_particles)
            splits = np.random.choice(num_particles, n_splits, replace=False)
            return splits

    class CollinearCluster(clustering_algorithm):
        def create_int_float_tables(self, start_ints, start_floats):
            no_start_parent = [row[self._col_num["Parent"]] == -1
                               for row in start_ints]
            num_particles = len(no_start_parent)
            # if some partents have been assigned the clustering
            # is already complete
            if np.all(no_start_parent) and num_particles > 1:
                # also, if there is only one particle it is either a
                # single particle jet or unclusterable anyway,
                splits = get_splits(num_particles)
                start_floats = np.array(start_floats)
                start_ints = np.array(start_ints)
                ints1, floats1, ints2, floats2 = collenar_splits(
                    start_floats[splits, self._col_num["Energy"]],
                    start_floats[splits, self._col_num["Px"]],
                    start_floats[splits, self._col_num["Py"]],
                    start_floats[splits, self._col_num["Pz"]],
                    start_ints[splits, self._col_num["Label"]])
                start_ints[splits] = ints1
                start_ints = np.concatenate((start_ints, ints2))
                start_floats[splits] = floats1
                start_floats = np.concatenate((start_floats, floats2))
                start_ints = start_ints.tolist()
                start_floats = start_floats.tolist()
            return super().create_int_float_tables(start_ints, start_floats)
    return CollinearCluster


def check_correct_joins(eventWise, jet_name):
    eventWise.selected_index = None
    label = getattr(eventWise, jet_name + "_Label")
    parent = getattr(eventWise, jet_name + "_Parent")
    changed = np.zeros(len(label), dtype=bool)
    for event_n, (event_label, event_parent) in enumerate(zip(label, parent)):
        event_label = ak.flatten(event_label)
        event_parent = ak.flatten(event_parent)
        negative_mask = event_label < -1
        negative_inverts = -event_label[negative_mask]
        negative_sort = np.argsort(negative_inverts)
        negative_parent = event_parent[negative_mask][negative_sort]
        positive_mask = np.fromiter((label in negative_inverts
                                     for label in event_label+2),
                                    dtype=bool)
        positive_sort = np.argsort(event_label[positive_mask])
        positive_parent = event_parent[positive_mask][positive_sort]
        changed[event_n] = np.all(positive_parent == negative_parent)
    return changed


def single_check(eventWise, jet_class, jet_name, jet_params={}, num_splits=None):
    print("Clustering with splits")
    cluster_algorithm = clustering_with_collinear(jet_class, num_splits)
    jet_params["MaxMeanDist"] = np.inf
    clustered = FormJets.cluster_multiapply(eventWise=eventWise,
                                            cluster_algorithm=cluster_algorithm,
                                            dict_jet_params=jet_params,
                                            jet_name=jet_name,
                                            batch_length=np.inf,
                                            silent=False)
    print("Checking joins")
    changed = check_correct_joins(eventWise, jet_name)
    eventWise.append(**{jet_name + "_CollinearChanges": changed})
    if np.any(changed):
        print("Not collinear safe")
    else:
        print("Collinear safe")
