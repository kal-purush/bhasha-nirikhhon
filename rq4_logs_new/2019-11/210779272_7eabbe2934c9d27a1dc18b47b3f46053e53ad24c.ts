import request from '@/utils/request';
import { AxiosPromise } from 'axios';

import { IFile } from './types';

/**
 * @summary Method used to prepare file form
 * @param {File} file File
 * @param {string} name File name
 * @param {string} type File type (type in internships-manager system)
 */
export const sendFile = (file: File, name: string, type: string) => {
  const form = new FormData();
  form.append('file', file);
  form.set('name', name);
  form.set('type', type);

  return request({
    url: '/files',
    method: 'post',
    data: form,
    headers: { 'Content-Type': 'multipart/form-data' },
  }) as AxiosPromise<IFile>;
};