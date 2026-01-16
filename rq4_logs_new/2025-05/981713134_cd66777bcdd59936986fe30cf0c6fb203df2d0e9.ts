import { Test, TestingModule } from '@nestjs/testing';
import { FtnWorkerController } from './ftn-worker.controller';
import { FtnWorkerService } from './ftn-worker.service';

describe('FtnWorkerController', () => {
  let ftnWorkerController: FtnWorkerController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [FtnWorkerController],
      providers: [FtnWorkerService],
    }).compile();

    ftnWorkerController = app.get<FtnWorkerController>(FtnWorkerController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(ftnWorkerController.getHello()).toBe('Hello World!');
    });
  });
});