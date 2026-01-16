import { Mongoose } from 'mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { AppModule } from '../src/app.module';

let mongoConnection: Mongoose;

beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
    }).compile();

    const app = moduleFixture.createNestApplication();
    await app.init();

    mongoConnection = app.get<Mongoose>(Mongoose);
});

afterAll(async () => {
    await mongoConnection.disconnect();
});