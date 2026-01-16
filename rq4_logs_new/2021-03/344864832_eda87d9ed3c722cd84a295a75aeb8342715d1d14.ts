import { gfs } from '../index';
import mongoose from 'mongoose';
import express from 'express';

class GfsService {
    async getItem(filename: string, res: express.Response) {
        await gfs.find({ filename }).toArray((_err: any, files: any) => {
            if (!files || files.length === 0) {
                return res.status(404).send({
                    type: 'ERROR',
                    message: 'No files exist'
                });
            }
            gfs.openDownloadStreamByName(filename).pipe(res);
        });
    }

    async deleteItem(filename: string, res: express.Response, newFileName?: string) {
        await gfs.find({ filename }).toArray(async (_err: any, file: any) => {
            if (!file || file.length === 0) {
                return res.status(404).send({
                    type: 'ERROR',
                    message: 'No files exist'
                });
            }
            for (let k = 0; k < file.length; k++) {
                if ((newFileName && newFileName !== file[k].filename) || !newFileName) {
                    await gfs.delete(new mongoose.Types.ObjectId(file[k]._id));
                }
            }
        });
    }
}

export const gfsService = new GfsService();