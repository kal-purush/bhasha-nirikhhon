from .tensorboard_data_manager import TensorboardDataManager
from .wandb_data_manager import WandbDataManager
from .void_manager import VoidManager


def data_manager_factory(config: dict):
    data_manager_type = config["logging"]["data_manager"]
    if data_manager_type == "tensorboard":
        return TensorboardDataManager(config)
    elif data_manager_type == "wandb":
        return WandbDataManager(config)
    elif data_manager_type == "skip":
        return VoidManager(config)

    raise ValueError(f"Data Manager {data_manager_type} not supported.")