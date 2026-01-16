import axios from './axiosConfig';

export const apiUploadImage = (data: FormData, config = {}): Promise<Res> =>
   axios({
      url: '/image',
      method: 'post',
      data,
      ...config,
   });
export const apiDeleteImage = (filename: string): Promise<Res> =>
   axios({
      url: `/image/${filename}`,
      method: 'delete',
   });