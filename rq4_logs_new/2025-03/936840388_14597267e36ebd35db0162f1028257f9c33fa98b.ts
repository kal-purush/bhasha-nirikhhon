import request from 'supertest';
import appInit from '../server';
import mongoose from 'mongoose';
import path from 'path';
import fs from 'fs';
import { Express } from 'express';

let app: Express;

beforeAll(async () => {
    console.log('Before all tests');
    app = await appInit();
});

afterAll(async () => {
    console.log('After all tests');
    await mongoose.connection.close();
});

describe("Upload Route Tests", () => {
    test("Should return 400 if no file is uploaded", async () => {
        const response = await request(app)
            .post('/api/upload')
            .set('Content-Type', 'multipart/form-data');
        
        expect(response.statusCode).toBe(400);
        expect(response.body.error).toBe('No file uploaded');
    });

    test("Should upload an image successfully", async () => {
        const testImagePath = path.join(__dirname, 'test-image.jpg');

        // Create a dummy image file for testing
        fs.writeFileSync(testImagePath, Buffer.alloc(100));

        const response = await request(app)
            .post('/api/upload')
            .attach('file', testImagePath)
            .set('Content-Type', 'multipart/form-data');
        
        console.log('Upload response:', response.body);

        expect(response.statusCode).toBe(200);
        expect(response.body).toHaveProperty('url');
        expect(response.body).toHaveProperty('filename');
        expect(response.body).toHaveProperty('originalname');
        expect(response.body).toHaveProperty('mimetype');
        expect(response.body).toHaveProperty('size');

        // Cleanup test file
        fs.unlinkSync(testImagePath);
    });

    test("Should reject invalid file types", async () => {
        const testFilePath = path.join(__dirname, 'test.txt');
        fs.writeFileSync(testFilePath, 'This is a test file');

        const response = await request(app)
            .post('/api/upload')
            .attach('file', testFilePath)
            .set('Content-Type', 'multipart/form-data');
        
        expect(response.statusCode).toBe(500);
        expect(response.body.error).toContain('Invalid file type');

        // Cleanup test file
        fs.unlinkSync(testFilePath);
    });
});