import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as supertest from 'supertest';
import { AppModule } from '../app.module';
describe('BankManagerController', () => {
    let app: INestApplication;
  
    beforeEach(async () => {
      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
  
      app = moduleRef.createNestApplication();
      await app.init();
    });
  
    test('should create a bank manager', () => {
      const name = 'Tarcila Amaral';
      const cpf = '12354874516';
  
  
      return supertest(app.getHttpServer())
        .post('/bankManager')
        .send({
          name,
          cpf
        })
        .expect(201)
        .expect(({ body }) => {
          expect(body._name).toBe('Tarcila Amaral');
          expect(body._cpf).toBe(cpf);
        });
    });
  
    test('should return a bank manager', () => {
      const id = '3a665d0e-8775-49ea-bc83-b72d6deb9b48';
  
  
      return supertest(app.getHttpServer())
        .get(`/bankManager/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._name = "Ana Maria"
          body._cpf = "87534598765"
          body._isActive = false
          body._bankManagerToken = "3a665d0e-5489-49ea-bc83-b72d6deb9b48"
        });
    });
  
    test('should update a bank manager', () => {
      const id = '33430c83-4730-408e-a94d-cbf014162530';
      const name = "Luisa Oliveira"
      const cpf = "75421598547"
  
      return supertest(app.getHttpServer())
        .put(`/bankManager/${id}`)
        .send({
          name,
          cpf,
        })
        .expect(200)
        .expect(({ body }) => {
          body._name = name
          body._cpf = cpf
          body._isActive = true
          body._bankManagerToken = "8e739a3f-2c3b-4105-9ddb-185da5108de4"
        });
    });
  
    test('should deactivate a bank manager', () => {
      const id = '3a665d0e-8775-49ea-bc83-b72d6deb9b48';
  
      return supertest(app.getHttpServer())
        .delete(`/bankManager/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._isActive = false
        });
    });
  
  });