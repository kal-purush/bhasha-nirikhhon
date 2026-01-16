import os
import h5py
import numpy as np
import pandas as pd
from tqdm import tqdm
from random import sample
from PIL import Image

from torch.utils.data import Dataset, DataLoader
from torchvision import transforms, utils


from ..utils.hashsplit import hashsplit
from .DataProviderABC import DataProviderABC


# these are a few of utility functions that might be better stored elsewhere
def eight_bit_to_float(im, dtype=np.uint8):
    imax = np.iinfo(dtype).max  # imax = 255 for uint8
    im = im / imax
    return im

def load_h5(h5_path, channel_inds):
    f = h5py.File(h5_path, 'r')
    image = f['image'].value[channel_inds, ::]
    image = eight_bit_to_float(image)
    return image

def load_rgb_img(img_path, channel_inds, data_as_image=False):
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
    with open(img_path, 'rb') as f:
        with Image.open(f) as img:
            img_arr = np.array(img.convert('RGB'))
            if data_as_image:
                zero_channels = [i for i in range(3) if i not in channel_inds]
                img_arr[:,:,zero_channels] = 0
                return Image.fromarray(img_arr)
            else:
                return eight_bit_to_float(img_arr[:,:,channel_inds])
        
def load_greyscale_tiff(img_path,data_as_image=False):
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
    with open(img_path, 'rb') as f:
        with Image.open(f) as img:
            if data_as_image:
                return img
            else:
                return eight_bit_to_float(np.array(img.convert('L')))


# this is an augmentation to PyTorch's Dataset class that our Dataprovider below will use
class dataframeDataset(Dataset):
    """dataframe Dataset."""

    def __init__(self,
                 df,
                 data_root_dir='/root/aics/modeling/gregj/results/ipp/ipp_17_12_03/',
                 data_path_col='save_h5_reg_path',
                 data_type='hdf5',
                 data_as_image=False,
                 target_col='structureProteinName',
                 convert_target_to_string=True,
                 unique_id_col='save_h5_reg_path',
                 channel_inds=(3, 4, 2),
                 transform=None,
                 return_sample_as_dict=True):
        """
        Args:
            df (pandas.DataFrame): dataframe containing the image relative locations and target data
            data_root_dir (string): full path to the directory containing all the images.
            data_path_col (string): column name in dataframe containing the paths to the files to be used as input data.
            data_type (string): hdf5, png, etc (only hdf5 support is implemented)
            data_as_image (bool): if True, return a pil image, if False, return a np array of floats
	    target_col (string): column name in the dataframe containing the data to be used as prediction targets (no paths).
            convert_target_to_string (bool): force conversion of target col to string
            unique_id_col (string): which column in the dataframe file to use as a unique identifier for each data point
            channel_inds (tuple of integers): 0: cell segmentation
                                              1: nuclear segmentation
                                              2: DNA channel
                                              3: membrane channel
                                              4: structure channel
                                              5: bright-field
            transform (callable, optional): Optional transform to be applied on a sample.
            return_sample_as_dict (bool) if true return data, target, unique_id as entries in a dict with those keys, else return a tuple
        """
        opts = locals()
        opts.pop('self')
        self._opts = opts
        
        # check that all files we need are present in the df
        good_rows = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc='scanning files'):
            data_path = os.path.join(data_root_dir, df.ix[idx, data_path_col])
            if os.path.isfile(data_path):
                good_rows += [idx]
        print('dropping {0} rows out of {1} from the dataframe'.format(len(df) - len(good_rows), len(df)))
        df = df.iloc[good_rows]
        df = df.reset_index(drop=True)

        # force conversion of target col content sto string if desired
        if convert_target_to_string:
            df[target_col] = df[target_col].astype(str)
        
        # save df
        self.df = df

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        data_path = os.path.join(self._opts['data_root_dir'], self.df.ix[idx, self._opts['data_path_col']])
        if self._opts['data_type'] == 'hdf5':
            data = load_h5(data_path, self._opts['channel_inds'])
        elif self._opts['data_type'] in ['jpg','JPG','jpeg','JPEG','png','PNG','ppm','PPM','bmp','BMP']:
            data = load_rgb_img(data_path, self._opts['channel_inds'], data_as_image=self._opts['data_as_image'])
        elif self._opts['data_type'] in ['tif', 'TIF', 'tiff', 'TIFF']:
            data = load_greyscale_tiff(data_path, data_as_image=self._opts['data_as_image'])
        else:
            raise ValueError('data_type {} is not supported, only hdf5 and basic images'.format(self.data_type))

        if self._opts['data_as_image']:
            if self._opts['transform']:
                trans = self._opts['transform']
                data = trans(data)

        target = self.df.ix[idx, self._opts['target_col']]
        unique_id = self.df.ix[idx, self._opts['unique_id_col']]

        if self._opts['return_sample_as_dict']:
            sample = {'data': data, 'target': target, 'unique_id': unique_id}
        else:
            sample = (data,target,unique_id)
        
        return sample

    
# This is the dataframe-image dataprovider
class dataframeDataProvider(DataProviderABC):
    """dataframe dataprovider"""
    def __init__(self,
                 df,
                 data_root_dir='/root/aics/modeling/gregj/results/ipp/ipp_17_12_03/',
                 batch_size=32,
                 data_path_col='save_h5_reg_path',
                 data_type='hdf5',
                 data_as_image=False,
                 target_col='structureProteinName',
                 convert_target_to_string=True,
                 unique_id_col='save_h5_reg_path',
                 channel_inds=(3, 4, 2),
                 split_fracs={'train': 0.8, 'test': 0.2},
                 split_seed=1,
                 transform=None,
                 num_workers=4,
                 pin_memory=True,
                 return_sample_as_dict=True):
        """
        Args:
            df (pandas.DataFrame): dataframe containing the relative image locations and target data
            data_root_dir = (string): full path to the directory containing all the images.
            batch_size (int): minibatch size for iterating through dataset
            data_path_col (string): column name in dataframe containing the paths to the files to be used as input data.
            data_type (string): hdf5, png, jpg, tiff, etc (only hdf5 and common image format + tiff support is implemented)
            data_as_image (bool): if True, return a PIL image, else return a numpy array of floats
            target_col (string): column name in the dataframe containing the data to be used as prediction targets (no paths).
            convert_target_to_string (bool): force conversion of target column contents to string type
            unique_id_col (string): which column in the dataframe to use as a unique identifier for each data point
            channel_inds (tuple of integers): 0: cell segmentation
                                              1: nuclear segmentation
                                              2: DNA channel
                                              3: membrane channel
                                              4: structure channel
                                              5: bright-field
            split_fracs (dict): names of splits desired, and fracion of data in each split
            split_seed (int): random seed/salt for splitting has function
            transform (callable, optional): Optional transform to be applied on a sample.
            num_workers (int): number of cpu cores to use loading data
            pin_memory (Bool): should be True unless you're getting gpu memory errors
            return_sample_as_dict (bool) if true return data, target, unique_id as entries in a dict with those keys, else return a tuple
        """

        # save the input options a la greg's style
        opts = locals()
        opts.pop('self')
        self.opts = opts

        # split the data into the different sets: test, train, valid, whatever
        self._split_inds = hashsplit(df[self.opts['unique_id_col']],
                                     splits=split_fracs,
                                     salt=split_seed)

        # split dataframe by split inds
        dfs = {split:df.iloc[inds].reset_index(drop=True) for split,inds in self._split_inds.items()}

        # load up all the data, before splitting it -- the data gets checked in this call
        self._datasets = {split:dataframeDataset(df_s,
                                                 data_root_dir=data_root_dir,
                                                 data_path_col=data_path_col,
                                                 data_type=data_type,
                                                 data_as_image=data_as_image,
                                                 target_col=target_col,
                                                 convert_target_to_string=convert_target_to_string,
                                                 unique_id_col=unique_id_col,
                                                 channel_inds=channel_inds,
                                                 transform=transform,
                                                 return_sample_as_dict=return_sample_as_dict) for split,df_s in dfs.items()}
 
        # save filtered dfs as an accessable dict
        self.df = {split:dset.df for split,dset in self._datasets.items()}
        
        # create data loaders to efficiently iterate though the random samples
        self.dataloaders = {split:DataLoader(dset,
                                             batch_size=batch_size,
                                             num_workers=num_workers,
                                             pin_memory=pin_memory) for split, dset in self._datasets.items()}
       
        # save a map from unique ids to splits
        splits2ids = {split:df_s[self.opts['unique_id_col']] for split,df_s in self.df.items()}
        self._ids2splits = {v:k for k in splits2ids for v in splits2ids[k]} 
    
    @property
    def splits(self):
        return self._split_inds
    
    @property
    def classes(self):
        #return np.unique( self.df[self.opts['target_col']] ).tolist()
        unique_classes = np.union1d(*[df_s[self.opts['target_col']].unique() for split,df_s in self.df.items()])
        if np.any(np.isnan(unique_classes)):
            unique_classes = np.append(unique_classes[~np.isnan(unique_classes)], np.nan)
        return unique_classes.tolist()


    # WARNING: get_data_points is an inefficient way to interact with the data,
    # mosty useful for inspecting good/bad predictions post-hoc.
    # To efficiently iterate thoguh the data, use somthing like:
    #
    # for epoch in range(N_epochs):
    #     for phase, dataloader in dp.dataloaders.items():
    #         for i_mb,sample_mb in enumerate(dataloader):
    #             data_mb, targets_mb, unique_ids_mb = sample_mb['data'], sample_mb['target'], sample_mb['unique_id']
    def get_data_points(self, unique_ids):
        ids2splits = {uid:self._ids2splits[uid] for uid in unique_ids}
        ids2splitsandinds = {uid:(ids2splits[uid], self.df[ids2splits[uid]].index[self.df[ids2splits[uid]][self.opts['unique_id_col']]] == uid ) for uid in unique_ids}
        data_points = [self._dataset[split][ind[0]] for uid,(split,ind) in unique_ids.items()]
        #df_inds = self.df.index[self.df[self.opts['unique_id_col']].isin(unique_ids)].tolist()
        #data_points = [self._dataset[i] for i in df_inds]
        return data_points