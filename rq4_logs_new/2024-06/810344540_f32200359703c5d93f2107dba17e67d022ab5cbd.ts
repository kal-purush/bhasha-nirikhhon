import { Injectable, Logger } from '@nestjs/common';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { v4 as uuid } from 'uuid';
import { UploadFile } from 'src/common/interfaces/upload-file.interface';

@Injectable()
export class S3Service {
  private readonly s3Client: S3Client;
  private readonly bucketName = process.env.AWS_S3_BUCKET_NAME;
  private readonly logger = new Logger(S3Service.name);

  constructor() {
    this.s3Client = new S3Client({ region: process.env.AWS_REGION });
  }

  async uploadFile(file: UploadFile): Promise<string> {
    const key = `${uuid()}-${file.originalname}`;
    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: key,
      Body: file.buffer,
      ContentType: file.mimetype,
    });

    try {
      await this.s3Client.send(command);
      return key;
    } catch (error) {
      this.logger.error('Failed to upload file to S3', error);
      throw error;
    }
  }
}