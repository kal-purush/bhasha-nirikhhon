import { diskStorage } from 'multer';
import { resolve } from 'path';
import { randomBytes } from 'crypto';

const tempPath = resolve(__dirname, '..', '..', 'tmp');

export default {
  dest: tempPath,
  storage: diskStorage({
    destination: (req: any, file: any, callback) => {
      callback(null, tempPath)
    },
    filename: (req: any, file: any, callback) => {
      randomBytes(16, (error, hash) => {
        error && callback(error, '');
        
        file.key = `${hash.toString('hex')}-${file.originalname}`;
        callback(null, file.key);
      })
    },
  }),
};