import { Injectable } from '@nestjs/common';
import * as AWS from 'aws-sdk';

@Injectable()
export class S3BucketService {
  private s3: AWS.S3;
  private readonly bucketName = process.env.AWS_S3_BUCKET_NAME;

  constructor() {
    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
    });
    this.s3 = new AWS.S3();
  }

  async uploadImage(file: Express.Multer.File): Promise<string | null> {
    const fileKey = `${Date.now()}_${file.originalname}`;

    const uploadParams = {
      Bucket: this.bucketName,
      Key: fileKey,
      Body: file.buffer,
      ContentType: file.mimetype,
    };

    try {
      const { Location } = await this.s3.upload(uploadParams).promise();

      return Location;
    } catch (error) {
      throw new Error('Erro ao fazer upload da imagem no S3: ' + error.message);
    }
  }
}