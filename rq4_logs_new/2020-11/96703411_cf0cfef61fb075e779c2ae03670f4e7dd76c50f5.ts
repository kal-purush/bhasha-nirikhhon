
export interface FilesystemStorageOptions {
  rootDir: string;
}

export interface S3StorageOptions {
  clientOptions: {
    credentials: {
      accessKeyId: string;
      secretAccessKey: string;
    };
    region: string;
  };
  bucket: string;
  endpoint: string;
}