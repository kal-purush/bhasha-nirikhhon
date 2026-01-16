from PyQt5.QtGui import QPixmap
from PyQt5 import QtWidgets

class Controls:
    def __init__(self):
        self.brightness_name = "Brightness"
        self.contrast_name = "Contrast"
        self.noise_name = "Noise"
        self.brightness_init = 0
        self.contrast_init = 0
        self.noise_init = 0
        self.brightness_min = -100
        self.brightness_max = 100
        self.contrast_min = -100
        self.contrast_max = 100
        self.noise_min = 0
        self.noise_max = 100
        self.cur_brightness_value = self.brightness_init
        self.cur_contrast_value = self.contrast_init
        self.cur_noise_value = self.noise_init

    def subscribe_view(self, view) -> None:
        self.view = view
    def subscribe_image_handler(self, image_handler):
        self.image_handler = image_handler
    def subscribe_modifications_provider(self, modifications_provider):
        self.modifications_provider = modifications_provider

    def update_brightness(self, value: float):
        if self.image_handler.modified_image is not None:
            self.modifications_provider.add_change({'name': 'brightness', 'value': float(value)})
            self.modifications_provider.apply_changes()
            self.cur_brightness_value = float(value)
            self.view.MeasuredImage.setPixmap(
                QPixmap(self.image_handler.convert_matrix_to_qimage(self.image_handler.modified_image)))

    def update_contrast(self, value: float):
        if self.image_handler.modified_image is not None:
            self.modifications_provider.add_change({'name': 'contrast', 'value': float(value)})
            self.modifications_provider.apply_changes()
            self.cur_contrast_value = float(value)
            self.view.MeasuredImage.setPixmap(
                QPixmap(self.image_handler.convert_matrix_to_qimage(self.image_handler.modified_image)))

    def update_noise(self, value: float):
        if self.image_handler.modified_image is not None:
            self.modifications_provider.add_change({'name': 'noise', 'value': float(value)})
            self.modifications_provider.apply_changes()
            self.cur_noise_value = float(value)
            self.view.MeasuredImage.setPixmap(
                QPixmap(self.image_handler.convert_matrix_to_qimage(self.image_handler.modified_image)))

    def loadImage(self):
        if self.image_handler is None:
            raise Exception("loadImage", "image_handler not set")
        elif self.modifications_provider is None:
            raise Exception("loadImage", "modifications_provider not set")
        path_to_image, _ = QtWidgets.QFileDialog.getOpenFileName(None, "Choose image", "",
                                                                 "Image Files (*.png *.jpg *.bmp)")
        if path_to_image != '':
            self.view.ContrastSlider.setSliderPosition(self.contrast_init)
            self.view.BrightnessSlider.setSliderPosition(self.brightness_init)
            self.view.NoiseSlider.setSliderPosition(self.noise_init)
            self.modifications_provider.reset_changes()
            self.image_handler.load_image_from_file(path_to_image)
            self.image_handler.regenerate_view()             