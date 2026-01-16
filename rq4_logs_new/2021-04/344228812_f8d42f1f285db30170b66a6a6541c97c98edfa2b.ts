import DynamoDB, { PutItemInputAttributeMap } from 'aws-sdk/clients/dynamodb';
import { GetENVOrThrow } from '../../util/setup';

export type GalleryTablePutItemInput = {
  primaryKey: string,
  secondaryKey?: string,
  attributes: PutItemInputAttributeMap
}

export class GalleryTable {
  private static DB = new DynamoDB()

  private static TABLE_NAME = GetENVOrThrow('GALLERY_TABLE_NAME');

  static async PutItem(props: GalleryTablePutItemInput) : Promise<void> {
    const {
      primaryKey,
      secondaryKey,
      attributes,
    } = props;
    const itemKeys : PutItemInputAttributeMap = {
      pk: {
        S: primaryKey,
      },
    };
    if (props.secondaryKey) {
      itemKeys.sk = {
        S: secondaryKey,
      };
    }
    await this.DB.putItem({
      TableName: this.TABLE_NAME,
      Item: {
        ...itemKeys,
        ...attributes,
      },
    }).promise();
  }
}