import tkinter as tk
from tkinter import filedialog, Label, Button, messagebox
from PIL import Image, ImageTk, UnidentifiedImageError
import torch
from torchvision import models
from torchvision.models import ResNet18_Weights, MobileNet_V2_Weights
from torchvision import transforms
import json
import urllib


# Function to download and load ImageNet class labels from a URL
def load_imagenet_labels():
    """
    Downloads and loads ImageNet labels for classification from a URL.
    Returns:
        labels (list): A list of class labels used for classification.
    """
    url = "https://raw.githubusercontent.com/anishathalye/imagenet-simple-labels/master/imagenet-simple-labels.json"
    response = urllib.request.urlopen(url)
    labels = json.loads(response.read())
    return labels


# Base class for the Tkinter window
class BaseWindow(tk.Tk):
    """
    A base class for initializing a Tkinter window.
    Inheritance and Encapsulation principles are used.

    Args:
        title (str): The title of the window.
    """

    def __init__(self, title="Image Classifier"):
        super().__init__()
        self.title(title)
        self.geometry("800x600")


# Main class for Image Classification Application
class ImageClassifierApp(BaseWindow):
    """
    Image Classification application using ResNet and MobileNet models.
    Demonstrates Multiple Inheritance and Polymorphism.

    Args:
        title (str): The title of the window (default is "Image Classifier").
    """

    def __init__(self, title="Image Classifier"):
        super().__init__(title)
        # Load the ImageNet class labels
        self.imagenet_labels = load_imagenet_labels()

        # Initialize ResNet18 as the default model
        self.model = models.resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)
        self.model.eval()  # Set model to evaluation mode (disable training)

        # Call function to create and display widgets
        self.create_widgets()

    def create_widgets(self):
        """
        Creates and displays the widgets in the window, including buttons,
        labels, and image preview.
        """
        # Button to upload image
        self.upload_button = Button(
            self, text="Upload Image", command=self.upload_image
        )
        self.upload_button.pack(pady=10)

        # Label and radio buttons for model selection
        self.model_label = Label(self, text="Select Classification Model:")
        self.model_label.pack(pady=5)

        self.model_selection = tk.StringVar(value="ResNet18")  # Default model selection

        # Radio button for ResNet18 model
        self.resnet_button = tk.Radiobutton(
            self,
            text="ResNet18",
            variable=self.model_selection,
            value="ResNet18",
            command=self.switch_model,
        )
        self.resnet_button.pack()

        # Radio button for MobileNet model
        self.mobilenet_button = tk.Radiobutton(
            self,
            text="MobileNet",
            variable=self.model_selection,
            value="MobileNet",
            command=self.switch_model,
        )
        self.mobilenet_button.pack()

        # Label to display the classification result
        self.result_label = Label(
            self, text="Classification Result: ", font=("Helvetica", 16)
        )
        self.result_label.pack(pady=20)

        # Label to preview the uploaded image
        self.image_label = Label(self)
        self.image_label.pack()

    def switch_model(self):
        """
        Switch between ResNet18 and MobileNet models based on user selection.
        Demonstrates Polymorphism by dynamically changing the model.
        """
        selected_model = self.model_selection.get()
        if selected_model == "ResNet18":
            self.model = models.resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)
        elif selected_model == "MobileNet":
            self.model = models.mobilenet_v2(weights=MobileNet_V2_Weights.IMAGENET1K_V1)
        self.model.eval()  # Ensure the model is in evaluation mode

    def upload_image(self):
        """
        Opens a file dialog to allow the user to upload an image.
        The image is displayed and classified using the selected model.
        """
        file_path = filedialog.askopenfilename(
            filetypes=[("JPEG files", "*.jpg"), ("PNG files", "*.png")]
        )

        # If no file is selected, show a warning message
        if not file_path:
            messagebox.showwarning("Warning", "No file selected!")
            return

        try:
            # Display the uploaded image and classify it
            self.display_image(file_path)
            self.classify_image(file_path)
        except UnidentifiedImageError:
            messagebox.showerror(
                "Error", "Unsupported image format. Please select a valid image."
            )
        except Exception as e:
            # Handle generic errors
            messagebox.showerror("Error", f"Failed to process the image: {str(e)}")

    def display_image(self, file_path):
        """
        Displays the uploaded image in the GUI window.

        Args:
            file_path (str): Path of the image to be displayed.
        """
        image = Image.open(file_path)

        # Resize image to fit the display area (using LANCZOS for better quality)
        image = image.resize((400, 300), Image.Resampling.LANCZOS)
        image = ImageTk.PhotoImage(image)

        # Update the image_label widget to show the image
        self.image_label.config(image=image)
        self.image_label.image = image  # Keep a reference to avoid garbage collection

    def classify_image(self, file_path):
        """
        Classifies the uploaded image using the selected AI model.

        Args:
            file_path (str): Path of the image to be classified.
        """
        image = Image.open(file_path)
        transformed_image = self.transform_image(image)

        with torch.no_grad():  # Disable gradient computation for inference
            output = self.model(transformed_image)

        # Get the predicted class
        _, predicted_class = output.max(1)
        class_name = self.get_class_name(predicted_class.item())

        # Update the result label with the classification result
        self.result_label.config(text=f"Classification Result: {class_name}")

    def transform_image(self, image):
        """
        Transforms the image into the required format (tensor) for classification.

        Args:
            image (PIL.Image): The image to be transformed.

        Returns:
            image_tensor (torch.Tensor): Transformed image tensor with batch dimension.
        """
        # Convert to RGB if the image has an alpha channel (e.g., PNG with transparency)
        if image.mode != "RGB":
            image = image.convert("RGB")

        # Define the transformation steps: resize, crop, convert to tensor, and normalize
        preprocess = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

        # Apply the transformations and add a batch dimension (1, C, H, W)
        image_tensor = preprocess(image)
        return image_tensor.unsqueeze(0)

    def get_class_name(self, class_idx):
        """
        Maps the class index to the corresponding human-readable class name.

        Args:
            class_idx (int): Index of the predicted class.

        Returns:
            class_name (str): Human-readable label for the predicted class.
        """
        if class_idx < len(self.imagenet_labels):
            return self.imagenet_labels[class_idx]
        return "Unknown"


# Entry point to start the application
if __name__ == "__main__":
    app = ImageClassifierApp("AI-Powered Image Classifier")
    app.mainloop()