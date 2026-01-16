import { afterAll, beforeAll, describe, expect, it } from '@jest/globals';
import fs from 'fs';
import { Db, MongoClient } from 'mongodb';
import mongodbConfig from '../../src/config/mongodb-config.json';

const inputPath = './tests/res/input.png';

describe('deleteImage', () => {
    let input: string;

    let client: MongoClient;
    let db: Db;
    let image_id: string;

    beforeAll(async () => {
        await Promise.all([
            new Promise<void>((resolve, reject) => {
                client = new MongoClient(`${mongodbConfig.development.ip}/?${Object.entries(mongodbConfig.development.options).map(([key, value]) => `${key}=${value}`).join('&')}`);
                db = client.db(mongodbConfig.development.db);
                client.connect().then(() => resolve()).catch(err => reject(err));
            }),
            new Promise<void>((resolve, reject) => {
                fs.readFile(inputPath, 'base64', (err, data) => {
                    if (err) reject(err);
                    input = data;
                    resolve();
                });
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
        const data = await response.json();
        image_id = data.image.image_id;
    });

    afterAll(async () => {
        // only if fails to delete
        await db.collection('images').deleteOne({ image_id });
        await client.close();
    });

    it('should return 404 if the user is not the author', async () => {
        const response = await fetch(`http://localhost:3801/api/images/image?image_id=${image_id}&user_id=not-test-user`, {
            method: 'DELETE'
        });
        const data = await response.json();
        expect(response.status).toBe(404);
        expect(data).toEqual({ error: 'image not found' });
    });

    it('should delete the image from the database and disk', async () => {
        const response = await fetch(`http://localhost:3801/api/images/image?image_id=${image_id}&user_id=test-user`, {
            method: 'DELETE'
        });
        const data = await response.json();
        expect(response.status).toBe(200);
        expect(data).toEqual({ message: 'image deleted' });
    });

    it('should return 404 if the image does not exist', async () => {
        const response = await fetch(`http://localhost:3801/api/images/image?image_id=${image_id}&user_id=test-user`, {
            method: 'DELETE'
        });
        const data = await response.json();
        expect(response.status).toBe(404);
        expect(data).toEqual({ error: 'image not found' });
    });
});