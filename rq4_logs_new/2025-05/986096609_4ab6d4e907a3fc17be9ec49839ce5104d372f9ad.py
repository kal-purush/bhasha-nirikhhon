# === Import necessary libraries ===
import streamlit as st                   # For building the web app interface
import tensorflow as tf                  # For deep learning operations
import numpy as np                       # For numerical operations
from PIL import Image                    # For image loading and conversion
import io                                # For in-memory file operations
import os                                # For checking if CSS file exists

# === Page Configuration ===
st.set_page_config(
    page_title="Neural Style Transfer",  # Web page title
    page_icon="🎨",                      # Tab icon
    layout="centered"                   # Center the layout
)

# === Loading the CSS Styles ===
def load_css():
    css_file = "style.css"      # Define the CSS file name
    if os.path.exists(css_file):         # Check if the CSS file exists
        with open(css_file, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)  # Apply CSS styling
    else:
        st.warning("⚠️ CSS file not found.")  # Show warning if CSS is missing

load_css()

# === Title and Subtitle ===
st.markdown("<h1 class='main-title'>🎨 Neural Style Transfer</h1>", unsafe_allow_html=True)
st.markdown("<p class='subtitle'>Turn your content image into art using the power of neural networks!</p>", unsafe_allow_html=True)

# === Helper Functions ===

# Load and resize image, return as tensor
def load_img(path):
    max_dim = 512                         # Resize image to max 512 pixels
    img = Image.open(path)                # Open image using PIL
    img.thumbnail((max_dim, max_dim))     # Resize while keeping aspect ratio
    img = np.array(img)                   # Convert to NumPy array
    img = tf.convert_to_tensor(img, dtype=tf.float32)  # Convert to TensorFlow tensor
    img = tf.expand_dims(img, axis=0)     # Add batch dimension
    return img

# Convert processed tensor image back to displayable format
def deprocess_img(processed_img):
    x = processed_img.squeeze()           # Remove batch dimension
    x = tf.clip_by_value(x, 0, 255)       # Ensure pixel values are in valid range
    x = tf.cast(x, tf.uint8)              # Convert to integers
    return x.numpy()                      # Return as NumPy array for display

# Calculate content loss between generated and original content images
def compute_content_loss(base_content, target):
    return tf.reduce_mean(tf.square(base_content - target))  # Mean squared error

# Compute Gram matrix for capturing style
def gram_matrix(tensor):
    channels = int(tensor.shape[-1])      # Number of feature maps (channels)
    a = tf.reshape(tensor, [-1, channels])  # Flatten height and width
    n = tf.shape(a)[0]                    # Total number of positions
    gram = tf.matmul(a, a, transpose_a=True)  # Matrix multiplication
    return gram / tf.cast(n, tf.float32)  # Normalize Gram matrix

# Calculate style loss using Gram matrices
def compute_style_loss(base_style, gram_target):
    gram_style = gram_matrix(base_style)  # Gram of generated image
    return tf.reduce_mean(tf.square(gram_style - gram_target))  # Mean squared error

# Calculate total loss: weighted sum of content and style losses
def compute_loss(model, loss_weights, init_image, gram_style_features, content_features):
    style_weight, content_weight = loss_weights

    model_outputs = model(init_image)     # Get style and content from model
    style_output_features = model_outputs[:len(style_layers)]
    content_output_features = model_outputs[len(style_layers):]

    style_score = 0
    content_score = 0

    # Style loss for each style layer
    for target_style, comb_style in zip(gram_style_features, style_output_features):
        style_score += compute_style_loss(comb_style, target_style)

    # Content loss for each content layer
    for target_content, comb_content in zip(content_features, content_output_features):
        content_score += compute_content_loss(comb_content, target_content)

    # Weight the losses
    style_score *= style_weight
    content_score *= content_weight

    return style_score + content_score     # Total loss

# === Upload Images Section ===
st.subheader("📷 Upload Content Image")
content_image_file = st.file_uploader("Choose Content Image", type=["jpg", "jpeg", "png"])

st.subheader("🖌️ Upload Style Image")
style_image_file = st.file_uploader("Choose Style Image", type=["jpg", "jpeg", "png"])

# === Start Style Transfer Button ===
if st.button("✨ Start Style Transfer"):
    if content_image_file is not None and style_image_file is not None:
        with st.spinner('🔀 Processing... Please wait...'):

            # Load content and style images from uploaded files
            content_path = content_image_file
            style_path = style_image_file
            content_image = load_img(content_path)
            style_image = load_img(style_path)

            # Load pre-trained VGG19 model without top layer (for features only)
            vgg = tf.keras.applications.VGG19(include_top=False, weights='imagenet')
            vgg.trainable = False          # Freeze model parameters

            # Define layers to extract features for style and content
            content_layers = ['block5_conv2']
            style_layers = ['block1_conv1', 'block2_conv1', 'block3_conv1',
                            'block4_conv1', 'block5_conv1']
            num_content_layers = len(content_layers)
            num_style_layers = len(style_layers)

            # Create a model that outputs content and style layers
            def get_model():
                outputs = [vgg.get_layer(name).output for name in style_layers + content_layers]
                return tf.keras.Model([vgg.input], outputs)

            model = get_model()

            # Extract features from content and style images
            def get_feature_representations(model, content_path, style_path):
                content_image = load_img(content_path)
                style_image = load_img(style_path)
                style_outputs = model(style_image)
                content_outputs = model(content_image)
                style_features = [style_layer for style_layer in style_outputs[:len(style_layers)]]
                content_features = [content_layer for content_layer in content_outputs[len(style_layers):]]
                return style_features, content_features

            # Get style and content features
            style_features, content_features = get_feature_representations(model, content_path, style_path)
            gram_style_features = [gram_matrix(style_feature) for style_feature in style_features]

            # Initialize the image to be optimized (start from content image)
            init_image = tf.Variable(content_image, dtype=tf.float32)

            # Define optimizer and weights for style/content
            opt = tf.keras.optimizers.Adam(learning_rate=5.0)
            style_weight = 1e-2
            content_weight = 1e4
            loss_weights = (style_weight, content_weight)

            # Training loop for optimization
            epochs = 50
            progress_bar = st.progress(0)  # Progress bar for UI

            for epoch in range(epochs):
                with tf.GradientTape() as tape:
                    loss = compute_loss(model, loss_weights, init_image, gram_style_features, content_features)
                grad = tape.gradient(loss, init_image)
                opt.apply_gradients([(grad, init_image)])
                clipped = tf.clip_by_value(init_image, 0.0, 255.0)  # Keep pixel values valid
                init_image.assign(clipped)
                progress_bar.progress((epoch + 1) / epochs)  # Update progress

            # Convert tensor to image for display
            final_img = deprocess_img(init_image.numpy())
            st.subheader("🎉 Final Styled Image")
            st.image(final_img, width=500)

            # Prepare image for download
            final_pil_img = Image.fromarray(final_img)
            buffer = io.BytesIO()
            final_pil_img.save(buffer, format="PNG")
            buffer.seek(0)

            # Download button
            st.download_button(
                label="📅 Download Styled Image",
                data=buffer,
                file_name="styled_image.png",
                mime="image/png"
            )

            st.success("✅ Style Transfer Completed Successfully!")
    else:
        # Warn if either image is missing
        st.warning('⚠️ Please upload both Content and Style images!')