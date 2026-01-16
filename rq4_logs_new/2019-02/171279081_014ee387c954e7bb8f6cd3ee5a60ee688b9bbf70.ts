import { ConfigService } from './../config.service';
import { MongooseModule } from '@nestjs/mongoose';
import { ArticleModule } from './article.module';
import { Test } from '@nestjs/testing';
import { ArticleService } from './article.service';
import { ArticleController } from './article.controller';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import * as testData from './tests.data.json';

describe('ArticleController', () => {
    let app: INestApplication;

    let articleService = { findAll: () => testData.getArticle};

    beforeAll(async () => {
        const module = await Test.createTestingModule({
            controllers: [ArticleController],
            imports: [MongooseModule.forRootAsync({
                useClass: ConfigService
            }), ArticleModule],
        })
          .overrideProvider(ArticleService)
          .useValue(articleService)
          .compile();
    
        app = module.createNestApplication();
        await app.init();
      });
    
    it(`/GET article`, () => {
      return request(app.getHttpServer())
        .get('/article')
        .expect(200)
        .expect(articleService.findAll());
    });

    afterAll(async () => {
      await app.close();
    });
})