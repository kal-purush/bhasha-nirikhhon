import { afterAll, beforeAll, describe, expect, it } from '@jest/globals';
import crypto from 'crypto';
import fs from 'fs';
import { Db, MongoClient } from 'mongodb';
import mongodbConfig from '../../src/config/mongodb-config.json';

const inputPath = './tests/res/input.png';
const outputDir = './tests/images/albums';

describe('getImage', () => {
    let input: string;
    let inputHash: string;

    let client: MongoClient;
    let db: Db;
    let image_id: string;

    beforeAll(async () => {
        await Promise.all([
            // connect to the database
            new Promise<void>((resolve, reject) => {
                client = new MongoClient(`${mongodbConfig.development.ip}/?${Object.entries(mongodbConfig.development.options).map(([key, value]) => `${key}=${value}`).join('&')}`);
                db = client.db(mongodbConfig.development.db);
                client.connect().then(() => {
                    console.log('connected to mongodb db');
                    resolve();
                }).catch(err => reject(err));
            }),
            // load the image
            new Promise<void>((resolve, reject) => {
                fs.readFile(inputPath, 'base64', (err, data) => {
                    if (err) reject(err);
                    input = data;
                    inputHash = crypto.createHash('MD5').update(input).digest('hex');
                    resolve();
                });
            }),
        ]);

        // upload the image
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
        return db.collection('images').deleteOne({ image_id });
    });

    it('should fetch the image data', async () => {
        const response = await fetch(`http://localhost:3801/api/images/image?image_id=${image_id}`);
        const text = await response.text();
        const imageHash = crypto.createHash('MD5').update(text).digest('hex');
        expect(imageHash).toEqual(inputHash);
    });
});