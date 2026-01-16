import { Test, TestingModule } from '@nestjs/testing';
import { bankManagerController } from './bankManager.controller';

describe('bankManagerController', () => {
  let controller: bankManagerController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [bankManagerController],
    }).compile();

    controller = module.get<bankManagerController>(bankManagerController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});