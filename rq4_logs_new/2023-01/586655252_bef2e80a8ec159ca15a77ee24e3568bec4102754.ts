import { S3, PutObjectCommandInput } from '@aws-sdk/client-s3';
import { fromBuffer } from 'file-type';
import env from '../config/env';

export default class UsersS3Repository {
  private s3: S3;

  constructor() {
    this.s3 = new S3({
      region: 'ap-northeast-2',
      credentials: {
        accessKeyId: env.AWS_S3_ACCESS_KEY_ID,
        secretAccessKey: env.AWS_S3_SECRET_ACCESS_KEY,
      },
    });
  }

  async putProfileImage(image: Buffer) {
    const fileType = await fromBuffer(image);
    if (!fileType) throw new Error('이미지 변환 과정에서 문제가 생겼습니다.');

    const key = `profileImages/${Date.now().toString()}.${fileType.ext}`;
    const bucket = 'da-vinci-code-game-bucket';
    const contentType = fileType.mime;

    const objectInput: PutObjectCommandInput = {
      Bucket: bucket,
      Key: key,
      Body: image,
      ContentType: contentType,
      ACL: 'public-read',
    };

    await new Promise((resolve, reject) => {
      this.s3.putObject(objectInput, (err, objectOutput) => {
        if (err) reject(err);
        resolve(objectOutput);
      });
    });

    return `https://${bucket}.s3.ap-northeast-2.amazonaws.com/${key}`;
  }

  protected delete(bucket: string, key: string) {
    return this.s3.deleteObject({ Bucket: bucket, Key: key });
  }

  async deleteObjectByUrl(url: string) {
    const [, bucket, key] = url.split(
      /https:\/\/|http:\/\/|\.s3\.ap-northeast-2\.amazonaws\.com\//
    );

    return this.delete(bucket, key);
  }
}