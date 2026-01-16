from argparse import ArgumentParser

import pytorch_lightning as pl
from pytorch_lightning.callbacks import LearningRateMonitor
from pytorch_lightning.loggers import WandbLogger
import wandb

from supervised_FCN_2.experiments.exp_train import ExpFCN
from supervised_FCN_2.preprocessing.data_pipeline import build_data_pipeline
from pytorch_lightning.callbacks import EarlyStopping
from supervised_FCN_2.utils import *


def load_args():
    parser = ArgumentParser()
    parser.add_argument('--config', type=str, help="Path to the config data  file.",
                        default=get_root_dir().joinpath('configs', 'config.yaml'))
    parser.add_argument('--archive_name', type=str, help="UCR or UEA`.", default='UCR')
    parser.add_argument('--dataset_names', nargs='+', help="e.g., Adiac Wafer Crop`.", default='')
    # parser.add_argument('--gpu_device_ind', nargs='+', default=[0], type=int)
    return parser.parse_args()


if __name__ == '__main__':
    # load config
    args = load_args()
    config = load_yaml_param_settings(args.config)

    archive_name = args.archive_name
    dataset_name = args.dataset_names
    
    # fit
    for dataset_name in args.dataset_names:
        print('dataset_name:', dataset_name)

        # dataset
        train_data_loader, test_data_loader = [build_data_pipeline(archive_name, dataset_name, config, kind) for kind in ['train', 'test']]
        
        # fit
        train_exp = ExpFCN(config, len(train_data_loader.dataset), len(np.unique(train_data_loader.dataset.Y)))
        wandb_logger = WandbLogger(project='supervised-FCN-2', name=dataset_name, config=config)
        trainer = pl.Trainer(logger=wandb_logger,
                             enable_checkpointing=False,
                             callbacks=[LearningRateMonitor(logging_interval='step'), 
                                        EarlyStopping(monitor='val/loss', mode='min', patience=10, verbose=False)],
                             accelerator='gpu',
                             **config['trainer_params'])
        trainer.fit(train_exp,
                    train_dataloaders=train_data_loader,
                    val_dataloaders=test_data_loader,)
        # test
        trainer.test(train_exp, test_data_loader)
        save_model({f"{dataset_name}": train_exp.fcn})
        
        wandb.finish()
