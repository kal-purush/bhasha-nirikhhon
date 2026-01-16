import imageCompression from 'browser-image-compression';

const imageCompressionOptions = {
  maxSizeMB: 1,
  maxWidthOrHeight: 1920,
};

export const compressImage = async (image: File) => {
  try {
    const compressedFile = await imageCompression(image, imageCompressionOptions);
    return compressedFile;
  } catch (error) {
    console.log(error);
    return;
  }
};