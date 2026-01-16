import { GalleryBucket } from '../../services/s3';

export type AddGalleryEntryProps = {
  title: string;
  description: string;
  imageData: Buffer;
};

export type GalleryImageSummary = {
  title: string;
  description: string;
  imageUrl: string;
};

export default class Gallery {
  static async AddToGallery(
    userId: string,
    props: AddGalleryEntryProps,
  ): Promise<GalleryImageSummary> {
    const { title, description, imageData } = props;
    const imageKey = `${Date.now()}-${Math.floor(Math.random() * 100000)}.jpg`;
    const { imageUrl } = await GalleryBucket.UploadImage(imageKey, imageData);
    return {
      title,
      description,
      imageUrl,
    };
  }
}