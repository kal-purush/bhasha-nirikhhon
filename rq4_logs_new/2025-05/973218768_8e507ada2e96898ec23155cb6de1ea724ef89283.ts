import { Injectable, InternalServerErrorException, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

@Injectable()
export class StorageService {
  private readonly logger = new Logger(StorageService.name);
  private readonly s3: S3Client;
  private readonly bucketName: string;
  constructor(private readonly configService: ConfigService) {
    this.s3 = new S3Client({
      region: configService.get('region'),
      credentials: {
        accessKeyId: configService.get('key'),
        secretAccessKey: configService.get('secret'),
      },
    });
    this.bucketName = configService.get('name');
  }

  async uploadFile(key: string, body: Buffer, contentType: string) {
    try {
      const command = new PutObjectCommand({
        Bucket: this.bucketName,
        Key: key,
        Body: body,
        ContentType: contentType,
      });
      await this.s3.send(command);
      this.logger.log(`Documents ${key} uploaded to storage succsessfully`);
    } catch (error) {
      this.logger.error('File upload failed:', error);
      throw new InternalServerErrorException('Failed to upload file');
    }
  }

  async getPresignedUrl(key: string) {
    try {
      const command = new GetObjectCommand({
        Bucket: this.bucketName,
        Key: key,
      });
      this.logger.log(`Url for document ${key} received succsessfully`);
      return await getSignedUrl(this.s3, command, { expiresIn: 3600 });
    } catch (error) {
      this.logger.error('Failed to get file url:', error);
      throw new InternalServerErrorException('Failed to get file url');
    }
  }

  async deleteFile(key: string): Promise<void> {
    try {
      this.logger.log(`Deleting file: ${key}...`);

      const command = new DeleteObjectCommand({
        Bucket: this.bucketName,
        Key: key,
      });

      await this.s3.send(command);

      this.logger.log(`File deleted successfully: ${key}`);
    } catch (error) {
      this.logger.error(`Failed to delete file: ${key}`, error);
      throw new InternalServerErrorException('Failed to delete file');
    }
  }
}