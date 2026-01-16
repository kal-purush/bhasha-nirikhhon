import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ExampleEntity } from './entities/example.entity';

const MockAppService = jest.fn<Partial<AppService>, []>(() => ({
  get: jest.fn(),
}));

describe('AppController', () => {
  let appController: AppController;
  let appService: AppService;

  beforeEach(async () => {
    appService = MockAppService() as AppService;
    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [{ provide: AppService, useValue: appService }],
    }).compile();

    appController = app.get<AppController>(AppController);
  });

  describe('get', () => {
    it('should return successful response', async () => {
      const id = 1;
      const expected: ExampleEntity = { id, value: 'some value' };
      appService.get = jest.fn().mockResolvedValue(expected);

      expect(await appController.get(id)).toBe(expected);
    });
  });
});