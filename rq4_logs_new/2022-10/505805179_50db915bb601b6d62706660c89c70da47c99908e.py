import os
import yaml
class DataSelector():
    """Class that lets you generate bash scripts that will create "virtual" folders
    that can be used by the L2CS-Net data loaders. No data is copied anywhere, the original
    folders stay the same, but instead symbolic links are created and annotations files
    are accordingly modified.
    """
    def __init__(self, data_folders_filepath: str, dataset_config_filepath: str):
        """Class constructor

        Args:
            data_folders_filepath: YAML file that will contain paths to folders that include
                                   already prepared GazeCapture-compatible data folders
                                   (folders with images + annotations.txt)
            dataset_config_filepath: YAML file that contains description on how the new
                                     dataset should be selected for training and validation
                                     datasets
        """

        assert os.path.isfile(data_folders_filepath)
        assert os.path.isfile(dataset_config_filepath)

        # load contents of data folders YAML file
        with open(data_folders_filepath) as f:
            self._data_folders_dict = yaml.safe_load(f)

        # load contents of dataset config YAML file
        with open(dataset_config_filepath) as f:
            self._dataset_config_dict = yaml.safe_load(f)

    def generate_dataset_creation_script(self):
        pass