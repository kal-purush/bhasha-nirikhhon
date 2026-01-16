import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from '../src/app.module';

describe('SearchBooksController (e2e)', () => {
  let app: INestApplication;
  let moduleFixture: TestingModule;

  beforeEach(async () => {
    moduleFixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  // テストで起動したNestアプリを終了しないとJestで警告が発生するため、以下のコードで終了
  afterEach(async () => {
    await app.close();
    await moduleFixture.close();
  });

  describe('findAll()', () => {
    it('検索結果が返ってくること', async () => {
      const { body } = await request(app.getHttpServer())
        .get('/searchBooks')
        .query({ keyword: 'プログラミング' })
        .expect(200);
      expect(!!body.length).toBeTruthy();
    });
  });
});