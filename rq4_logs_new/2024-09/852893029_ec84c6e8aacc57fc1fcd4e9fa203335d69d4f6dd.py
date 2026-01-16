import cv2
from gtts import gTTS
import pygame
import io
import torch
from PIL import Image
from transformers import BlipProcessor, BlipForConditionalGeneration

# Initialize Pygame mixer for audio playback
pygame.mixer.init()

# Load pre-trained BLIP model and processor
processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

# Function to preprocess image for BLIP model
def preprocess_image(image):
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    image = Image.fromarray(image)
    return image

# Function to generate caption from image
def generate_caption(image):
    image = preprocess_image(image)
    inputs = processor(images=image, return_tensors="pt")
    out = model.generate(**inputs)
    caption = processor.decode(out[0], skip_special_tokens=True)
    return caption

# Function to speak out the caption
def speak_out(text):
    tts = gTTS(text=text, lang='en')
    with io.BytesIO() as mp3_fp:
        tts.write_to_fp(mp3_fp)
        mp3_fp.seek(0)
        pygame.mixer.music.load(mp3_fp, 'mp3')
        pygame.mixer.music.play()
        while pygame.mixer.music.get_busy():
            pass  # Wait until the speech is done

# Load an input image
input_image_path = r"C:\Users\bhava\OneDrive\Desktop\Instant Narratives\sample.jpg"  # Replace with the path to your input image
image = cv2.imread(input_image_path)

if image is None:
    print(f"Error: Unable to load image from '{input_image_path}'")
else:
    # Generate caption and speak it out
    try:
        caption = generate_caption(image)
        print("Generated Caption:", caption)
        speak_out(caption)
    except Exception as e:
        print(f"Error: {e}")