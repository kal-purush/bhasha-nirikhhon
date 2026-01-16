import numpy as np


def align_to_gocue(spks: np.ndarray, gocues: np.ndarray, bins=100) -> np.ndarray: # spks: ( 734, 214, 250 )
    gocues = gocues.squeeze() # ( 214, )

    spks_aligned = []
    for trial in range(len(gocues)):
        gocue = int(gocues[trial] * 100)

        spk = spks[:, trial, gocue:gocue+bins] # ( 734, 100 )
        assert spk.shape[-1] == bins
        
        spks_aligned.append(spk)

    
    spks_aligned = np.stack(spks_aligned).transpose(1,0,2)

    return spks_aligned # ( 734, 214, 100 )


def split_to_regions(data: np.ndarray, regions: np.ndarray) -> dict:
    regions_unique = np.unique(regions)

    regions_idxs = [np.where(regions == region)[0] for region in regions_unique] # unwise to use np.where but I forgot 

    data_dict = {}
    for idxs, name in zip(regions_idxs, regions_unique):
        data_dict.update({name: data[idxs]})

    return data_dict