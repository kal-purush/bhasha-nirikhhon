import cloudinary from '../config/cloud';
import streamifier from 'streamifier';

/**
 * Uploads a buffer to Cloudinary in the given folder with the given publicId.
 * @param buffer - The image buffer to upload.
 * @param folder - The Cloudinary folder.
 * @param publicId - The desired public ID.
 * @returns A Promise resolving to the secure URL of the uploaded image.
 */
export const uploadToCloudinary = (
  buffer: Buffer,
  folder: string,
  publicId: string
): Promise<string> => {
  return new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      {
        folder,
        public_id: publicId,
        resource_type: 'image',
      },
      (error: unknown, result: any) => {
        if (error) {
          return reject(error);
        }
        if (!result || !result.secure_url) {
          return reject(new Error('No result from Cloudinary'));
        }
        resolve(result.secure_url);
      }
    );

    streamifier.createReadStream(buffer).pipe(stream);
  });
};

/**
 * Extracts the Cloudinary public ID from the given URL.
 * @param url - The Cloudinary image URL.
 * @returns The public ID string, or null if invalid.
 */
export const extractPublicId = (url: string): string | null => {
  try {
    const parts = url.split('/');
    const lastPart = parts.pop();
    if (!lastPart) return null;
    const [fileName] = lastPart.split('.');
    const uploadIndex = parts.indexOf('upload');
    if (uploadIndex === -1) return null;
    const folderPath = parts.slice(uploadIndex + 2).join('/');

    return `${folderPath}/${fileName}`;
  } catch (err) {
    return null;
  }
};

/**
 * Deletes an image from Cloudinary using its URL.
 * @param url - The Cloudinary image URL.
 * @param throwOnFail - Whether to throw an error on failure.
 */
export const deleteImage = async (
  url: string,
  throwOnFail = false
): Promise<void> => {
  const publicId = extractPublicId(url);
  if (!publicId) {
    const msg = 'Invalid Cloudinary URL';
    if (throwOnFail) throw new Error(msg);
    console.error(msg);
    return;
  }
  try {
    await cloudinary.uploader.destroy(publicId);
  } catch (err) {
    if (throwOnFail) throw new Error('Failed to delete image');
    console.error('Image deletion failed:', err);
  }
};