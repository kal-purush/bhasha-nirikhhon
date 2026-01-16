import numpy as np
import gradio as gr
from PIL import Image
import keras
from huggingface_hub import from_pretrained_keras

model = from_pretrained_keras("keras-io/lowlight-enhance-mirnet", compile=False)

def infer(original_image):
    image = keras.utils.img_to_array(original_image)
    image = image.astype("float32") / 255.0
    image = np.expand_dims(image, axis=0)
    output = model.predict(image)
    output_image = output[0] * 255.0
    output_image = output_image.clip(0, 255)
    output_image = output_image.reshape(
        (np.shape(output_image)[0], np.shape(output_image)[1], 3)
    )
    output_image = np.uint32(output_image)
    return output_image

iface = gr.Interface(
    fn=infer,
    title="Clarify",
    inputs=[gr.inputs.Image(label="image", type="pil", shape=(960, 640))],
    outputs="image").launch(enable_queue=True)