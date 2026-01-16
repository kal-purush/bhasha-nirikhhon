import os
from re import A
import cv2
from matplotlib import pyplot as plt

from modules.transform.transform_params import TransformParams


class ImageTransformer:
    def __init__(self, params: TransformParams):
        self.params = params

    def validate_image(self):
        if not self.params.image_path or not os.path.exists(self.params.image_path):
            return False, {"error": "Invalid image path"}, 400
        return True, None, None

    def load_image(self):
        image = cv2.imread(self.params.image_path)
        return cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

    def apply_transforms(self, image):
        transforms = []

        # Chuyển đổi cơ bản
        if self.params.transform_type == 'horizontal_flip':
            transforms.append(A.HorizontalFlip(p=self.params.p))
        elif self.params.transform_type == 'vertical_flip':
            transforms.append(A.VerticalFlip(p=self.params.p))
        elif self.params.transform_type == 'rotate_90':
            transforms.append(A.Rotate(limit=(self.params.limit, self.params.limit), p=self.params.p))

        # Thêm các phép biến đổi bạn yêu cầu
        if self.params.blur > 0:
            blur_limit = (self.params.blur, self.params.blur) if self.params.blur % 2 == 1 else (self.params.blur - 1, self.params.blur - 1)
            transforms.append(A.GaussianBlur(blur_limit=blur_limit, p=1.0))

        if self.params.sharpen > 0:
            transforms.append(A.Sharpen(p=1.0))
        
        if self.params.brightness != 1.0 or self.params.contrast != 1.0:
            transforms.append(A.RandomBrightnessContrast(brightness_limit=(self.params.brightness-1, self.params.brightness-1), contrast_limit=(self.params.contrast-1, self.params.contrast-1), p=1.0))
        
        # Phép biến đổi bổ sung
        transforms.append(A.MedianBlur(blur_limit=(self.params.blur, self.params.blur), p=1.0))
        transforms.append(A.GlassBlur(sigma=self.params.blur, p=1.0))
        transforms.append(A.ZoomBlur(p=self.params.p))
        transforms.append(A.ToFloat(max_value=255.0))
        transforms.append(A.ToGray(p=self.params.p))
        transforms.append(A.ToSepia(p=self.params.p))
        transforms.append(A.ChannelShuffle(p=self.params.p))
        transforms.append(A.CLAHE(clip_limit=2.0, tile_grid_size=(8, 8), p=self.params.p))
        transforms.append(A.ColorJitter(brightness=self.params.brightness, contrast=self.params.contrast, p=self.params.p))
        transforms.append(A.Downscale(scale_min=0.25, scale_max=0.75, p=self.params.p))
        transforms.append(A.Emboss(p=self.params.p))
        transforms.append(A.Equalize(p=self.params.p))
        transforms.append(A.FancyPCA(alpha=0.1, p=self.params.p))
        transforms.append(A.GaussNoise(var_limit=(self.params.noise, self.params.noise), p=self.params.p))
        transforms.append(A.HueSaturationValue(hue_shift_limit=20, sat_shift_limit=30, val_shift_limit=20, p=self.params.p))
        transforms.append(A.ImageCompression(quality_lower=60, quality_upper=100, p=self.params.p))
        transforms.append(A.InvertImg(p=self.params.p))
        transforms.append(A.ISONoise(color_shift=(0.01, 0.05), intensity=(0.1, 0.5), p=self.params.p))
        transforms.append(A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225), p=self.params.p))
        transforms.append(A.PixelDropout(p=self.params.p))
        transforms.append(A.Posterize(num_bits=4, p=self.params.p))
        transforms.append(A.RandomFog(fog_coef_lower=0.3, fog_coef_upper=1.0, p=self.params.p))
        transforms.append(A.RandomGamma(gamma_limit=(80, 120), p=self.params.p))
        transforms.append(A.RandomGridShuffle(grid=(3, 3), p=self.params.p))
        transforms.append(A.ShiftScaleRotate(shift_limit=0.0625, scale_limit=0.1, rotate_limit=45, p=self.params.p))
        transforms.append(A.RandomRain(p=self.params.p))
        transforms.append(A.RandomShadow(p=self.params.p))
        transforms.append(A.RandomSnow(p=self.params.p))
        transforms.append(A.RandomSunFlare(src_radius=50, p=self.params.p))
        transforms.append(A.RandomToneCurve(scale=0.5, p=self.params.p))
        transforms.append(A.RGBShift(r_shift_limit=20, g_shift_limit=20, b_shift_limit=20, p=self.params.p))
        transforms.append(A.RingingOvershoot(p=self.params.p))
        transforms.append(A.UnsharpMask(p=self.params.p))
        transforms.append(A.Solarize(p=self.params.p))
        transforms.append(A.Spatter(p=self.params.p))
        transforms.append(A.Superpixels(p_replace=0.1, n_segments=100, p=self.params.p))
        transforms.append(A.Transpose(p=self.params.p))

        # Áp dụng các biến đổi
        if transforms:
            transform = A.Compose(transforms)
            augmented = transform(image=image)
            return augmented['image']
        
        return image

    def save_image(self, image, output_path):
        plt.imsave(output_path, image)
        return output_path