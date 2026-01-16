import { afterAll, beforeAll, describe, expect, it } from '@jest/globals';
import fs from 'fs';
import sizeOf from 'image-size';
import { Db, MongoClient } from "mongodb";
import mongodbConfig from '../../../src/config/mongodb-config.json';
import { getThumbnail } from "../../../src/routes/albums/Albums";

const inputPath = './tests/res/input.png';

describe('loadThumb', () => {

    let input: string;
    let image_id: string;

    let client: MongoClient;
    let db: Db;

    beforeAll(async () => {
        await Promise.all([
            new Promise<void>((resolve, reject) => {
                fs.readFile(inputPath, (err, data) => {
                    if (err) reject(err);
                    input = Buffer.from(data).toString('base64');
                    resolve();
                });
            }),
            new Promise<void>((resolve, reject) => {
                client = new MongoClient(`${mongodbConfig.development.ip}/?${Object.entries(mongodbConfig.development.options).map(([key, value]) => `${key}=${value}`).join('&')}`);
                db = client.db(mongodbConfig.development.db);
                client.connect().then(() => resolve()).catch(err => reject(err));
            })
        ]);

        const response = await fetch('http://localhost:3801/api/images/image', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                album_id: 'test-album',
                user_id: 'test-user',
                base64: input
            })
        });
        const body = await response.json();
        image_id = body.image.image_id;
    });

    afterAll(async () => {
        await db.collection('images').deleteOne({ image_id });
        await client.close();
    });

    it('should load a 128x128 thumbnail of a base64 encoded png from disk', async () => {
        const thumb = await getThumbnail('test-album', image_id);

        expect(thumb).not.toBeNull();
        const dimensions = sizeOf(Buffer.from(thumb!, 'base64'));

        expect(dimensions.width).toBe(128);
        expect(dimensions.height).toBe(128);
    });

    it('should return null if thumbnail does not exist', async () => {
        const thumb = await getThumbnail('test-album', 'non-existent-image-id');
        expect(thumb).toBeNull();
    });
});