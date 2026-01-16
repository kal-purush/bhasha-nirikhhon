import { useMutation, UseMutationOptions } from '@tanstack/react-query';
import axios from 'axios';
import { HikeError } from '../../errors/HikeError';

interface UploadFileToS3Params {
  file: File | Blob | Buffer;
  presignedUrl: string;
  onProgress?: (progress: number) => void;
}

export const useUploadFileToS3 = (
  mutationOptions?: Omit<UseMutationOptions<void, HikeError<null>, UploadFileToS3Params>, 'mutationKey' | 'mutationFn'>
) =>
  useMutation({
    mutationKey: ['uploadFileToS3'],
    mutationFn: async ({ file, presignedUrl, onProgress }: UploadFileToS3Params) => {
      await axios.put(presignedUrl, file, {
        headers: {
          'Content-Type': file instanceof File ? file.type : 'application/octet-stream'
        },
        onUploadProgress: (progressEvent) => {
          if (onProgress && progressEvent.total) {
            const progress = (progressEvent.loaded / progressEvent.total) * 100;
            onProgress(progress);
          }
        }
      });
    },
    ...mutationOptions
  });