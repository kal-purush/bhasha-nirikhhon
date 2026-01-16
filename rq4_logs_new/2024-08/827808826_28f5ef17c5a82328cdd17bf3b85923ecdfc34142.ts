import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as supertest from 'supertest';
import { AppModule } from '../app.module';
describe('poupancaAccountController', () => {
    let app: INestApplication;
  
    beforeEach(async () => {
      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
  
      app = moduleRef.createNestApplication();
      await app.init();
    });
  
    test('should create a poupanca account', () => {
      const isBankManager = true
      const amount = 2024;
      const initDate = '2024-11-20';
      const accountNumber = 4569
  
  
      return supertest(app.getHttpServer())
        .post('/poupancaAccounts')
        .send({
         isBankManager,
         accountNumber,
         amount,
         initDate,
        })
        .expect(201)
        .expect(({ body }) => {
          expect(body._amount).toBe(amount);
          expect(body._initDate).toBe(initDate);
          expect(body._accountNumber).toBe(accountNumber);
        });
    });
  
    test('should return a poupanca account', () => {
      const id = '31644825-b413-4f26-9d21-e88d42072813';
  
  
      return supertest(app.getHttpServer())
        .get(`/poupancaAccounts/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._amount = 256
          body._initDate = "2023-7-21"
          body._isActive = true
          body._accountNumber= "8965"
        });
    });
  
    test('should update a poupanca accounts', () => {
      const id = '54c84d54-64c1-4583-bb51-0939a8d007b1';
      const amount = 2250
      const initDate = "2022-4-14"
      const accountNumber = "7412"
  
      return supertest(app.getHttpServer())
        .put(`/poupancaAccounts/${id}`)
        .send({
          id,
          amount,
          initDate,
          accountNumber
        })
        .expect(200)
        .expect(({ body }) => {
          body._amount = amount
          body._initDate = initDate
          body._isActive = true
          body._accountNumber = accountNumber
        });
    });
  
    test('should deactivate a current account', () => {
      const id = 'dd8d8b0a-dd24-4920-a19e-0aa5d3424471';
  
      return supertest(app.getHttpServer())
        .delete(`/poupancaAccounts/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._isActive = false
        });
    });
  
  });