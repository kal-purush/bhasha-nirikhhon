import multer from 'multer';
import path from 'path';
import httpStatus from 'http-status-codes';

import { pdfFilter } from './filter';
import { APIError } from '../utils/error';

const baseDir = path.join(__dirname, '..', process.env.BASE_STORAGE_DIR);

export const multerPdfStorage = multer.diskStorage({
    destination(_req: any, _file, cb) {
        cb(null, baseDir);
    },
    filename(_req: any, file, cb) {
        if (file.originalname) {
            cb(
                null,
                file.originalname
                    .replace(/[^a-z0-9]/gi, '_')
                    .toLowerCase()
                    .replace(/__+/g, '-'),
            );
        } else {
            cb(new APIError('ID is required to upload a banner', httpStatus.BAD_REQUEST, 1), '');
        }
    },
});

export const uploadHandler = multer({
    storage: multerPdfStorage,
    fileFilter: pdfFilter,
    limits: {
        fileSize: 4000000, // 4 MB
        fields: 20,
        files: 1,
        parts: 20,
    },
});