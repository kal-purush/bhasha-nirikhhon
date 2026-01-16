import { MulterOptions } from '@nestjs/platform-express/multer/interfaces/multer-options.interface';
import { diskStorage } from 'multer';
import * as mime from 'mime';

import { IFile } from '../models/upload.models';

const SIZE_LIMITS: number = 1048576;

export const filesOptions: MulterOptions = {
  limits: {
    fileSize: SIZE_LIMITS,
  },
  storage: diskStorage({
    destination: './files/',
    filename: (_, file: IFile, callback) => {
      callback(null, `${Date.now().toString()}.${mime.getExtension(file.mimetype)}`);
    },
  }),
};