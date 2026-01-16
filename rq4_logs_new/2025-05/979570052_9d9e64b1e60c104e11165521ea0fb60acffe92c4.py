import logging

from google import genai
from google.genai import types


def make_gemini_image(
    api_key: str,
    model: str,
    prompt_start: str,
    prompt_end: str,
) -> bytes | None:
    client = genai.Client(api_key=api_key)

    prompt = f"{prompt_start} {prompt_end}"

    logging.info(f"Making an image with prompt: {prompt[:100]}...")

    try:
        response = client.models.generate_images(
            model=model,
            prompt=prompt,
            config=types.GenerateImagesConfig(
                number_of_images=1,
            ),
        )

        bytes = response.generated_images[0].image.image_bytes

        return bytes
    except Exception as e:
        logging.error(f"Error generating image: {e}")
        return None