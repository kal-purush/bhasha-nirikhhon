import { ConfigService } from './../config.service';
import { MongooseModule } from '@nestjs/mongoose';
import { ArticleModule } from './article.module';
import { MockArticleService } from './mock/mock.article.service.mock';
import { Test } from '@nestjs/testing';
import { Article } from './interfaces/article.interface';
import { Model } from 'mongoose';
import { ArticleService } from './article.service';
import { ArticleController } from './article.controller';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import * as testData from './tests.data.json';


describe('ArticleController', () => {
    let articleController: ArticleController;

    beforeEach(async () => {
        const module = await Test.createTestingModule({
            controllers: [ArticleController],
            providers: [{provide: ArticleService, useClass: MockArticleService}],
          }).compile();
    
        articleController = module.get<ArticleController>(ArticleController);
      });
    

    describe('findAll', () => {
        it('should return an array of articles', async () => {
            const result = ['test'];
            expect(await articleController.findAll()).toEqual(result);
        });
    });
});

describe('ArticleController e2e', () => {
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
