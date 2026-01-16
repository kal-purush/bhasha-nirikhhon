import { storage } from '@/appwrite';

export default async function getUrl(image: Image) {
  if (!image) return;
  const url = storage.getFilePreview(image.bucketId, image.fieldId);

  return url;
}