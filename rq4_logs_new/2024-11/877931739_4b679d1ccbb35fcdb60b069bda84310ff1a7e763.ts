import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
const request = require('supertest');
import { AppModule } from '../src/app.module';
import { UserInputModel } from 'src/features/users/api/models/input.models';

describe('UserController (e2e)', () => {
    let app: INestApplication;

    beforeAll(async () => {
        const moduleFixture: TestingModule = await Test.createTestingModule({
            imports: [AppModule],
        }).compile();

        app = moduleFixture.createNestApplication();
        await app.init();
    });

    afterAll(async () => {
        await app.close();
    });

    it('/sa/users (GET)', async () => {
        const response = await request(app.getHttpServer())
            .get('/sa/users')
            .set('Authorization', 'Basic ' + Buffer.from('admin:qwerty').toString('base64'))
            .expect(200);

        expect(response.body).toBeDefined();
    });

    it('/sa/users (POST)', async () => {
        const userInput: UserInputModel = {
            login: 'testuser',
            password: 'testpassword',
            email: 'testuser@example.com',
        };

        const response = await request(app.getHttpServer())
            .post('/sa/users')
            .set('Authorization', 'Basic ' + Buffer.from('admin:qwerty').toString('base64'))
            .send(userInput)
            .expect(201);

        expect(response.body).toBeDefined();
        expect(response.body.login).toEqual(userInput.login);
        expect(response.body.email).toEqual(userInput.email);
    });

    it('/sa/users/:id (DELETE)', async () => {
        const userInput: UserInputModel = {
            login: 'testuser2',
            password: 'testpassword2',
            email: 'testuser2@example.com',
        };

        const createResponse = await request(app.getHttpServer())
            .post('/sa/users')
            .set('Authorization', 'Basic ' + Buffer.from('admin:qwerty').toString('base64'))
            .send(userInput)
            .expect(201);

        const userId = createResponse.body.id;

        await request(app.getHttpServer())
            .delete(`/sa/users/${userId}`)
            .set('Authorization', 'Basic ' + Buffer.from('admin:qwerty').toString('base64'))
            .expect(204);

        await request(app.getHttpServer())
            .get(`/sa/users/${userId}`)
            .set('Authorization', 'Basic ' + Buffer.from('admin:qwerty').toString('base64'))
            .expect(404);
    });
});

describe('AuthController (e2e)', () => {
let app: INestApplication;

beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
    imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
});

afterAll(async () => {
    await app.close();
});

it('/auth/login (POST)', async () => {
    const userInput: UserInputModel = {
    login: 'testuser',
    password: 'testpassword',
    email: 'testuser@example.com',
};
    // Login user
    const response = await request(app.getHttpServer())
    .post('/auth/login')
    .send({ loginOrEmail: userInput.login, password: userInput.password })
    .expect(200);

    expect(response.body).toBeDefined();
    expect(response.body.accessToken).toBeDefined();
});

it('/auth/registration (POST)', async () => {
    const userInput: UserInputModel = {
    login: 'testuser2',
    password: 'testpassword2',
    email: 'testuser2@example.com',
};

    const response = await request(app.getHttpServer())
    .post('/auth/registration')
    .send(userInput)
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/registration-confirmation (POST)', async () => {
    const confirmationCode = 'some-confirmation-code'; // Replace with actual confirmation code

    const response = await request(app.getHttpServer())
    .post('/auth/registration-confirmation')
    .send({ code: confirmationCode })
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/registration-email-resending (POST)', async () => {
    const email = 'testuser@example.com';

    const response = await request(app.getHttpServer())
    .post('/auth/registration-email-resending')
    .send({ email })
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/password-recovery (POST)', async () => {
    const email = 'testuser@example.com';

    const response = await request(app.getHttpServer())
    .post('/auth/password-recovery')
    .send({ email })
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/new-password (POST)', async () => {
    const newPassword = 'newpassword';
    const recoveryCode = 'some-recovery-code'; // Replace with actual recovery code

    const response = await request(app.getHttpServer())
    .post('/auth/new-password')
    .send({ newPassword, recoveryCode })
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/refresh-token (POST)', async () => {
    const userInput: UserInputModel = {
    login: 'testuser',
    password: 'testpassword',
    email: 'testuser@example.com',
};

    // Register user first
    await request(app.getHttpServer())
    .post('/auth/registration')
    .send(userInput)
    .expect(204);

    // Confirm registration
    const confirmationCode = 'some-confirmation-code'; // Replace with actual confirmation code
    await request(app.getHttpServer())
    .post('/auth/registration-confirmation')
    .send({ code: confirmationCode })
    .expect(204);

    // Login user
    const loginResponse = await request(app.getHttpServer())
    .post('/auth/login')
    .send({ loginOrEmail: userInput.login, password: userInput.password })
    .expect(200);

    const refreshToken = loginResponse.headers['set-cookie'][0].split(';')[0].split('=')[1];

    const response = await request(app.getHttpServer())
    .post('/auth/refresh-token')
    .set('Cookie', `refreshToken=${refreshToken}`)
    .expect(200);

    expect(response.body).toBeDefined();
    expect(response.body.accessToken).toBeDefined();
});

it('/auth/logout (POST)', async () => {
    const userInput: UserInputModel = {
    login: 'testuser',
    password: 'testpassword',
    email: 'testuser@example.com',
};

    // Register user first
    await request(app.getHttpServer())
    .post('/auth/registration')
    .send(userInput)
    .expect(204);

    // Confirm registration
    const confirmationCode = 'some-confirmation-code'; // Replace with actual confirmation code
    await request(app.getHttpServer())
    .post('/auth/registration-confirmation')
    .send({ code: confirmationCode })
    .expect(204);

    // Login user
    const loginResponse = await request(app.getHttpServer())
    .post('/auth/login')
    .send({ loginOrEmail: userInput.login, password: userInput.password })
    .expect(200);

    const refreshToken = loginResponse.headers['set-cookie'][0].split(';')[0].split('=')[1];

    const response = await request(app.getHttpServer())
    .post('/auth/logout')
    .set('Cookie', `refreshToken=${refreshToken}`)
    .expect(204);

    expect(response.body).toBeDefined();
});

it('/auth/me (GET)', async () => {
    const userInput: UserInputModel = {
    login: 'testuser',
    password: 'testpassword',
    email: 'testuser@example.com',
    };

    // Login user
    const loginResponse = await request(app.getHttpServer())
    .post('/auth/login')
    .send({ loginOrEmail: userInput.login, password: userInput.password })
    .expect(200);

    const accessToken = loginResponse.body.accessToken;

    const response = await request(app.getHttpServer())
    .get('/auth/me')
    .set('Authorization', `Bearer ${accessToken}`)
    .expect(200);

    expect(response.body).toBeDefined();
    expect(response.body.login).toEqual(userInput.login);
    expect(response.body.email).toEqual(userInput.email);
});
});