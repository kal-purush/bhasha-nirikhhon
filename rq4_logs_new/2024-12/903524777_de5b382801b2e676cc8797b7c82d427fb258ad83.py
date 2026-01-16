import streamlit as st
import torch
import torchvision.transforms as transforms
from torchvision import models
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np

# Función para visualizar los kernels aprendidos por las capas convolucionales
def visualize_kernels(model):
    st.subheader("Kernels Visualizados")
    for name, module in model.named_modules():
        if isinstance(module, torch.nn.Conv2d):  # Identificar capas convolucionales
            st.write(f"Capa: {name}")
            kernels = module.weight.data.cpu().numpy()
            num_kernels = kernels.shape[0]

            fig, axes = plt.subplots(1, min(num_kernels, 8), figsize=(15, 5))
            for i, ax in enumerate(axes):
                if i >= num_kernels:
                    break
                kernel = kernels[i][0]  # Tomar el primer canal del kernel
                ax.imshow(kernel, cmap='viridis')
                ax.axis('off')
            st.pyplot(fig)

# Función para visualizar mapas de activación
def visualize_activations(model, image_tensor):
    st.subheader("Mapas de Activación")
    activations = []
    hooks = []

    # Función hook para capturar las activaciones
    def hook_fn(module, input, output):
        activations.append(output)

    # Registrar hooks en capas convolucionales
    for module in model.modules():
        if isinstance(module, torch.nn.Conv2d):
            hooks.append(module.register_forward_hook(hook_fn))

    # Pasar la imagen por el modelo
    model(image_tensor.unsqueeze(0))

    # Mostrar activaciones
    for i, activation in enumerate(activations):
        st.write(f"Activación de la capa {i + 1}")
        activation = activation.squeeze(0).detach().cpu().numpy()
        fig, axes = plt.subplots(1, min(activation.shape[0], 8), figsize=(15, 5))
        for j, ax in enumerate(axes):
            if j >= activation.shape[0]:
                break
            ax.imshow(activation[j], cmap='viridis')
            ax.axis('off')
        st.pyplot(fig)

    # Eliminar hooks
    for hook in hooks:
        hook.remove()

# Interfaz de la aplicación
st.title("Visualización de Kernels y Mapas de Activación en CNNs")

# Seleccionar un modelo preentrenado
model_option = st.selectbox("Selecciona un modelo preentrenado", ["ResNet18", "AlexNet", "VGG16"])
if model_option == "ResNet18":
    model = models.resnet18(pretrained=True)
elif model_option == "AlexNet":
    model = models.alexnet(pretrained=True)
elif model_option == "VGG16":
    model = models.vgg16(pretrained=True)

# Mostrar kernels
if st.button("Visualizar Kernels"):
    visualize_kernels(model)

# Subir una imagen
uploaded_file = st.file_uploader("Sube una imagen para analizar mapas de activación", type=["jpg", "png", "jpeg"])
if uploaded_file:
    image = Image.open(uploaded_file).convert("RGB")  # Asegurarse de que la imagen sea RGB
    st.image(image, caption="Imagen Cargada", use_container_width=True)
        
    # Preprocesar la imagen
    preprocess = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    image_tensor = preprocess(image)

    # Mostrar mapas de activación
    if st.button("Visualizar Mapas de Activación"):
        visualize_activations(model, image_tensor)