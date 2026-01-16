import { Test, TestingModule } from '@nestjs/testing';
import { clientController } from './client.controller';
import { INestApplication } from '@nestjs/common';
import * as supertest from 'supertest';
import { AppModule } from '../app.module';

describe('ClientController', () => {
  let app: INestApplication;

  beforeEach(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.init();
  });

  test('should create a client', () => {
    const name = 'John Doe';
    const cpf = '34567912345';


    return supertest(app.getHttpServer())
      .post('/client')
      .send({
        name,
        cpf
      })
      .expect(201)
      .expect(({ body }) => {
        expect(body.name).toBe('John Doe');
        expect(body.cpf).toBe(cpf);
      });
  });

});