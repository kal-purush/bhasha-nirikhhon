from PIL import Image
from io import BytesIO
from base_converter import BaseConverter
from logger_config import configure_logger

logger = configure_logger(__name__)

class DynamicImageConverter(BaseConverter):
    def __init__(self, input_stream: BytesIO, target_format: str):
        super().__init__(input_stream)
        self.target_format = target_format.lower()

    def convert(self, output_stream: BytesIO):
        print("here")
        image = Image.open(self.input_stream)
        logger.debug(f"image mode: {image}")

        # Handle transparency for formats that don't support it, if necessary
        if self.target_format == 'jpeg' and (image.mode in ('RGBA', 'LA') or ('transparency' in image.info)):
            logger.debug(f"target format {self.target_format} and image mode {image.mode}")

            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[3])  # 3 is the alpha channel
            image = background
        else:
            image = image.convert("RGB")

        logger.debug(f"image mode1: {image}")
        save_format = self.target_format.upper()
        if save_format == 'JPG':
            save_format = 'JPEG'

        image.save(output_stream, format=save_format)

        logger.info(f"Image converted to {self.target_format.upper()} format")
        output_stream.seek(0)
        return output_stream