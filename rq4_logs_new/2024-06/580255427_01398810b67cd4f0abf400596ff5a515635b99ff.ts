import { Injectable } from '@nestjs/common';
import { join, parse } from 'path';
import { v4 as uuidv4 } from 'uuid';
import { Stream } from 'stream';
import { createWriteStream, existsSync, mkdirSync } from 'fs';
import { Upload } from 'graphql-upload-ts';

@Injectable()
export class FileUploadService {
  constructor() {}

  /**
   * Uploads multiple files to a specified collection.
   *
   * @param {[Upload]} uploads - An array of Upload objects representing the files to be uploaded.
   * @param {string} collectionName - The name of the collection to upload the files to.
   * @return {Promise<Array<{collectionName: string, name: string, fileName: string, mimeType: string}>>} - A promise that resolves to an array of objects containing information about the uploaded files.
   */
  async uploadFiles(uploads: [Upload], collectionName: string) {
    const mediaList = [];

    for (const upload of uploads) {
      if (upload.file) {
        const {
          filename: fileName,
          mimetype: mimeType,
          createReadStream,
        } = upload.file;
        const stream = createReadStream();

        const uniqueFileName = await this.getUniqueFileName(fileName);
        const filePath = collectionName + '/' + uniqueFileName;

        await this.processFileUploading(filePath, stream);

        mediaList.push({
          collectionName: collectionName,
          name: fileName,
          fileName: uniqueFileName,
          mimeType: mimeType,
        });
      }
    }

    return mediaList;
  }

  /**
   * Processes the file uploading.
   *
   * @param {string} filePath - The path where the file should be uploaded.
   * @param {Stream} stream - The stream of the file to upload.
   * @return {Promise<void>} A promise that resolves once the file upload is complete.
   */
  async processFileUploading(filePath: string, stream: Stream): Promise<void> {
    const uploadDirectory = join(process.cwd(), 'wwwroot');
    const fullPath = join(uploadDirectory, filePath);

    const dir = fullPath.substring(0, fullPath.lastIndexOf('/'));
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    return new Promise((resolve, reject) => {
      const writeStream = createWriteStream(fullPath);
      stream.pipe(writeStream).on('finish', resolve).on('error', reject);
    });
  }

  /**
   * Generates a unique file name by appending a UUID key to the given file name.
   *
   * @param {string} fileName - The original file name.
   * @return {Promise<string>} - The unique file name with the UUID key appended.
   */
  async getUniqueFileName(fileName: string) {
    const fileExtension = parse(fileName).ext;
    const uuidKey = uuidv4().slice(0, 4);

    return `${fileName}-${uuidKey}${fileExtension}`;
  }
}