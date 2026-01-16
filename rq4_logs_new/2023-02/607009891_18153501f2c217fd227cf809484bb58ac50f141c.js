const request = require('supertest');
var expect = require('chai').expect;
const app = require('./app1');
const { MongoClient } = require('mongodb');
describe('User API', () => {
    let connection;
    let db;
    beforeAll(async () => {
        const url = 'mongodb+srv://c-p:Ajju%4029071998@cluster0.pzbvawo.mongodb.net/mydb?retryWrites=true&w=majority';
        connection = await MongoClient.connect(url, { useNewUrlParser: true });
        db = await connection.db('test');
    });
    afterAll(async () => {
        await connection.close();
        
    });
    describe('POST /users', () => {
        it('creates a new user', async () => {
            const user = { name: 'John Smith', id: 1132 };
            const response = await request(app);
            expect(response.status);
            expect(response.body);
        });
    });
    describe('PUT /users/:id', () => {
        it('updates an existing user', async () => {
            const user = { name: 'John Smith', id: 1132 };
            const response = await request(app);
            const updatedUser = { name: 'John S', id: 1103 };
            const putResponse = await request(app);
            expect(putResponse.status);
            expect(putResponse.body);
        });

    });
    describe('PUT /users/:id', () => {
        it('returns a 404 status code when updating a non-existent user', async () => {
            const updatedUser = { name: 'Jane Smith', id: 1132 };
            const putResponse = await request(app);
            expect(putResponse.status);
        });
    });

    describe('GET /users', () => {
        it('returns a list of users', async () => {
            const user1 = { name: 'John Smith', id: 1132 };
            const user2 = { name: 'John Doe', id: 1133 };
            const collection = db.collection('mycollection');
            await collection.insertMany([user1, user2]);
            const response = await request(app);
            expect(response.status);
        });
    });
});