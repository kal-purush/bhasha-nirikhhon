import { GalleryTable } from '../../services/dynamo_db/gallery_table';
import { GalleryTablePrimaryKey, GalleryTableSecondaryKey } from '../../services/dynamo_db/gallery_table_key';

export type LikePhotoProps = {
  ownerUserId: string,
  photoId: string,
  likedByUserId: string
}

export default class PhotoModel {
  static async LikePhoto(props: LikePhotoProps) : Promise<void> {
    const { ownerUserId, photoId, likedByUserId } = props;
    const primaryKey = GalleryTablePrimaryKey.userId(ownerUserId);
    const secondaryKey = GalleryTableSecondaryKey.photoId(photoId);
    await GalleryTable.UpdateItem({
      primaryKey,
      secondaryKey,
      UpdateExpression: 'ADD #column_name :new_entry',
      ExpressionAttributeNames: {
        '#column_name': 'likedBy',
      },
      ExpressionAttributeValues: {
        ':new_entry': {
          SS: [likedByUserId],
        },
      },
    });
  }
}