from PIL import Image
import numpy as np
from numpy import asarray
import copy


class ImageHandler:
    def __init__(self):
        self.reference_image = None
        self.modified_image = None
        self.view = None
        self.metrics_engine = None

    def subscribe_view(self, view):
        self.view = view

    def subscribe_metrics_engine(self, metrics_engine):
        self.metrics_engine = metrics_engine

    def load_image_from_file(self, image_path):
        loaded_image = Image.open(image_path)
        image_array = asarray(loaded_image)
        # Remove alpha channel if exists
        if image_array.shape[2] > 3:
            image_array = np.delete(image_array, 3, 2)
        self.reference_image = asarray(image_array)
        self.modified_image = copy.deepcopy(self.reference_image)

    def apply_modification(self, function):
        self.modified_image = function(self.modified_image)

    def regenerate_view(self):
        if self.view is not None:
            self.view.display_ref_image(self.reference_image)
        else:
            raise Exception("regenerate_view", "view is not set")

    def trigger_metrics_calculation(self):
        if self.metrics_engine is not None:
            self.metrics_engine.calculate_metrics(self.reference_image, self.modified_image)
        else:
            raise Exception("trigger_metrics_calculation", "metrics_engine is not set")




