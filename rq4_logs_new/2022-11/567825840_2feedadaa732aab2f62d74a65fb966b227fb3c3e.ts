// get('robots/');
// get('robots/:id');
// post('robots/');
// patch('robots/is');
// delete('robots/:id');

import mongoose from 'mongoose';
import request from 'supertest';
import { app } from '../app.js';
import { dbConnect } from '../DB.connect.js';
import { Robot } from '../entities/robot.Types.js';
import { UserModel } from '../entities/user.js';
import { createToken, TokenPayload } from '../Services/auth.js';

const setCollection = async () => {
    const usersMock = [
        { name: 'marcos', email: ' marcos@live.com', rol: 'user' },
        { name: 'pedro', email: ' pedro@live.com', rol: 'user' },
    ];

    await UserModel.deleteMany();
    await UserModel.insertMany(usersMock);
    await Robot.deleteMany();
    const data = await UserModel.find();
    const testIds = [data[0].id, data[1].id];
    return testIds;
};

describe('given an "app" with "/robot" routes', () => {
    describe('when i have connection to mongoDB', () => {
        let token: string;
        let ids: Array<string>;
        beforeEach(async () => {
            await dbConnect();
            ids = await setCollection();
            const payload: TokenPayload = {
                id: ids[0],
                name: 'marcos',
                role: 'user',
            };
            token = createToken(payload);
        });
        afterEach(async () => {
            await mongoose.disconnect();
        });

        test(' then the get to ulr /robots should sent status 200()', async () => {
            const response = await request(app).get('/robots');
            expect(response.status).toBe(200);
        });

        test(' then the get to ulr /robots should sent status 200(anothe way)', async () => {
            await request(app).get('/robots/').expect(200);
        });

        test(' then the get to ulr /robots/:id with invalid id should sent status 503', async () => {
            const response = await request(app).get('/robots/23');
            expect(response.status).toBe(503);
        });

        test(' then the get to ulr /robots/:id with invalid id should sent status 404', async () => {
            const response = await request(app).get(
                '/robots/637d0072d521a8f3246da659'
            );
            expect(response.status).toBe(404);
        });

        test('Then the post to url /robots without authorization should sent status 403', async () => {
            const response = await request(app)
                .post('/robots/')
                .send({ name: 'marcos' });
            expect(response.status).toBe(403);
        });
        test(' then the get to ulr /robots/:id without authorization id should sent status 201', async () => {
            const response = await request(app)
                .post('/robots')
                .set('Authorizarion', `Bearer ${token}`)
                .send({ name: 'marcos' });
            expect(response.status).toBe(503);
        });
        test(' then the get to ulr /robots/:id without authorization id should sent status 201', async () => {
            const response = await request(app)
                .patch('/robots/637d0072d521a8f3246da659')

                .send({ name: 'marcos' });
            expect(response.status).toBe(403);
        });
        test(' then the get to ulr /robots/:id without authorization id should sent status 201', async () => {
            const response = await request(app)
                .patch(`/robots/${ids[0]}`)
                .set('Authorizarion', `Bearer ${token}`)
                .send({ name: 'marcos' });
            expect(response.status).toBe(201);
        });
    });
});