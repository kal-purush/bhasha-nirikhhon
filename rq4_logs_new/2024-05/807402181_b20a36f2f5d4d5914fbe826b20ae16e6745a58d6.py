# image_processor.py

import cv2
import numpy as np
import matplotlib.pyplot as plt
import os

class MorphologicalOperations:
    def __init__(self, kernel_size=(15, 15)):
        """
        Initialize the morphological operations with a given kernel size.

        :param kernel_size: Size of the structuring element.
        """
        self.kernel = np.ones(kernel_size, np.uint8)

    def dilate(self, image, iterations=3):
        """
        Apply dilation to the given image.

        Dilation adds pixels to the boundaries of objects in an image.

        Formula: (A ⊕ B)(x, y) = max_{(i, j) ∈ B} A(x - i, y - j)

        :param image: Input binary image.
        :param iterations: Number of iterations for dilation.
        :return: Dilated image.
        """
        return cv2.dilate(image, self.kernel, iterations=iterations)

    def erode(self, image, iterations=3):
        """
        Apply erosion to the given image.

        Erosion removes pixels from the boundaries of objects in an image.

        Formula: (A ⊖ B)(x, y) = min_{(i, j) ∈ B} A(x + i, y + j)

        :param image: Input binary image.
        :param iterations: Number of iterations for erosion.
        :return: Eroded image.
        """
        return cv2.erode(image, self.kernel, iterations=iterations)

    def open(self, image, iterations=3):
        """
        Apply opening (erosion followed by dilation) to the given image.

        Opening removes small objects from the foreground of an image.

        Formula: (A ∘ B) = (A ⊖ B) ⊕ B

        :param image: Input binary image.
        :return: Opened image.
        """
        eroded = self.erode(image, iterations)
        return self.dilate(eroded, iterations)

    def close(self, image, iterations=3):
        """
        Apply closing (dilation followed by erosion) to the given image.

        Closing closes small holes in the foreground and connects disjoint objects.

        Formula: (A • B) = (A ⊕ B) ⊖ B

        :param image: Input binary image.
        :return: Closed image.
        """
        dilated = self.dilate(image, iterations)
        return self.erode(dilated, iterations)


class ImageProcessor:
    def __init__(self, filepaths):
        """
        Initialize the image processor with a list of file paths.

        :param filepaths: List of file paths to the images.
        """
        self.filepaths = filepaths
        self.images = [cv2.imread(filepath, cv2.IMREAD_GRAYSCALE) for filepath in filepaths]
        self.image_names = [os.path.basename(filepath).split('.')[0] for filepath in filepaths]
        self.morph_ops = MorphologicalOperations()

    def apply_operations(self):
        """
        Apply morphological operations (dilation, erosion, opening, closing) to the images.
        """
        self.dilated_images = [self.morph_ops.dilate(img) for img in self.images]
        self.eroded_images = [self.morph_ops.erode(img) for img in self.images]
        self.opened_images = [self.morph_ops.open(img) for img in self.images]
        self.closed_images = [self.morph_ops.close(img) for img in self.images]

    def adjust_contrast_and_brightness(self, image, contrast=1.5, brightness=50):
        """
        Adjust the contrast and brightness of an image.

        :param image: Input image.
        :param contrast: Contrast adjustment factor.
        :param brightness: Brightness adjustment factor.
        :return: Adjusted image.
        """
        return cv2.convertScaleAbs(image, alpha=contrast, beta=brightness)

    def overlay_difference(self, original, processed):
        """
        Overlay the difference between the original and processed images in color.

        :param original: Original image.
        :param processed: Processed image.
        :return: Image with differences highlighted.
        """
        diff = cv2.absdiff(original, processed)
        diff_colored = cv2.applyColorMap(diff, cv2.COLORMAP_JET)
        original_colored = cv2.cvtColor(original, cv2.COLOR_GRAY2BGR)
        return cv2.addWeighted(original_colored, 0.7, diff_colored, 0.3, 0)

    def add_border_and_label(self, image, label, color=(255, 0, 0)):
        """
        Add a border and label to the image.

        :param image: Input image.
        :param label: Label text.
        :param color: Border color.
        :return: Image with border and label.
        """
        border_size = 5
        image_with_border = cv2.copyMakeBorder(image, border_size, border_size, border_size, border_size,
                                               cv2.BORDER_CONSTANT, value=color)
        cv2.putText(image_with_border, label, (10, image_with_border.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                    color, 1, cv2.LINE_AA)
        return image_with_border

    def display_combined_images(self, title, original_images, processed_images, operation):
        """
        Display combined images (original and processed) with proper spacing and labels.

        :param title: Title for the displayed images.
        :param original_images: List of original images.
        :param processed_images: List of processed images.
        :param operation: The morphological operation name for labeling.
        """
        fig, axs = plt.subplots(2, len(original_images), figsize=(15, 9))
        fig.suptitle(title, fontsize=24)
        for i, (orig_img, proc_img, img_name) in enumerate(zip(original_images, processed_images, self.image_names)):
            orig_img_adj = self.adjust_contrast_and_brightness(orig_img)
            proc_img_adj = self.adjust_contrast_and_brightness(proc_img)
            overlay_img = self.overlay_difference(orig_img_adj, proc_img_adj)

            axs[0, i].imshow(orig_img_adj, cmap='gray')
            axs[0, i].set_title(f"{img_name}", fontsize=14)
            axs[0, i].axis('off')

            axs[1, i].imshow(overlay_img)
            axs[1, i].set_title(f"{operation}: {img_name}", fontsize=14)
            axs[1, i].axis('off')
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        plt.show()

    def show_results(self):
        """
        Show the results of the morphological operations.
        """
        self.display_combined_images("Dilation Results", self.images, self.dilated_images, "Dilated")
        self.display_combined_images("Erosion Results", self.images, self.eroded_images, "Eroded")
        self.display_combined_images("Opening Results", self.images, self.opened_images, "Opened")
        self.display_combined_images("Closing Results", self.images, self.closed_images, "Closed")

    def save_images(self, output_dir):
        """
        Save the processed images to the specified directory.

        :param output_dir: Directory to save the images.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for i, filepath in enumerate(self.filepaths):
            filename = filepath.split('/')[-1]
            cv2.imwrite(f"{output_dir}/dilated_{filename}", self.dilated_images[i])
            cv2.imwrite(f"{output_dir}/eroded_{filename}", self.eroded_images[i])
            cv2.imwrite(f"{output_dir}/opened_{filename}", self.opened_images[i])
            cv2.imwrite(f"{output_dir}/closed_{filename}", self.closed_images[i])

    def save_combined_images(self, output_dir):
        """
        Save the combined images (original and processed) to the specified directory.

        :param output_dir: Directory to save the images.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        padding = 10
        for i, filepath in enumerate(self.filepaths):
            filename = filepath.split('/')[-1].split('.')[0]
            combined_height = self.images[i].shape[0]
            combined_width = self.images[i].shape[1] * 5 + padding * 4
            combined_image = np.zeros((combined_height, combined_width), dtype=np.uint8)

            orig_img_adj = self.adjust_contrast_and_brightness(self.images[i])
            dilated_img_adj = self.adjust_contrast_and_brightness(self.dilated_images[i])
            eroded_img_adj = self.adjust_contrast_and_brightness(self.eroded_images[i])
            opened_img_adj = self.adjust_contrast_and_brightness(self.opened_images[i])
            closed_img_adj = self.adjust_contrast_and_brightness(self.closed_images[i])

            combined_image[:, :self.images[i].shape[1]] = orig_img_adj
            combined_image[:, self.images[i].shape[1] + padding:self.images[i].shape[1] * 2 + padding] = dilated_img_adj
            combined_image[:,
            self.images[i].shape[1] * 2 + padding * 2:self.images[i].shape[1] * 3 + padding * 2] = eroded_img_adj
            combined_image[:,
            self.images[i].shape[1] * 3 + padding * 3:self.images[i].shape[1] * 4 + padding * 3] = opened_img_adj
            combined_image[:, self.images[i].shape[1] * 4 + padding * 4:] = closed_img_adj

            combined_image_with_border = self.add_border_and_label(combined_image, f"Combined {filename}")

            cv2.imwrite(f"{output_dir}/{filename}_combined.png", combined_image_with_border)