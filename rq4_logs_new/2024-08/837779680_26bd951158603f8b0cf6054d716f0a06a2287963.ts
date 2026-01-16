import multer, { StorageEngine } from 'multer';
import { Request } from 'express';
import path from 'path';

// Define Multer storage engine
const storage: StorageEngine = multer.diskStorage({
  destination: (req: Request, file: Express.Multer.File, cb: (error: Error | null, destination: string) => void) => {
    cb(null, './images');
  },
  filename: (req: Request, file: Express.Multer.File, cb: (error: Error | null, filename: string) => void) => {
    const fileExtension = path.extname(file.originalname);
    cb(null, `temp${fileExtension}`);
  },
});

// Create Multer instance with defined storage
const upload = multer({ storage });

export default upload;