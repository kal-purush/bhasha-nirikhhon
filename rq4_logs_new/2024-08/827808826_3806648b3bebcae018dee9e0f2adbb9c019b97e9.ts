import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as supertest from 'supertest';
import { AppModule } from '../app.module';
describe('CurrentAccountController', () => {
    let app: INestApplication;
  
    beforeEach(async () => {
      const moduleRef: TestingModule = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
  
      app = moduleRef.createNestApplication();
      await app.init();
    });
  
    test('should create a current account', () => {
      const isBankManager = true
      const amount = 2014;
      const initDate = '2023-11-20';
      const accountNumber = 4569
  
  
      return supertest(app.getHttpServer())
        .post('/currentAccounts')
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
  
    test('should return a current account', () => {
      const id = '408f683e-65dd-449b-b753-e019402ad645';
  
  
      return supertest(app.getHttpServer())
        .get(`/currentAccounts/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._amount = 2200
          body._initDate = "2023-5-25"
          body._isActive = false
          body._accountNumber= "9841"
        });
    });
  
    test('should update a current accounts', () => {
      const id = 'c6640507-e052-4053-b079-ca7ac61a2c7d';
      const amount = 2250
      const initDate = "2023-4-17"
      const accountNumber = "2324"
  
      return supertest(app.getHttpServer())
        .put(`/currentAccounts/${id}`)
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
      const id = 'c6640507-e052-4053-b079-ca7ac61a2c7d';
  
      return supertest(app.getHttpServer())
        .delete(`/currentAccounts/${id}`)
        .expect(200)
        .expect(({ body }) => {
          body._isActive = false
        });
    });
  
  });