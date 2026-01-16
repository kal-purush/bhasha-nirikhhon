import { Test, TestingModule } from '@nestjs/testing';
import {beforeEach, describe, expect, it, jest} from '@jest/globals';

import { ExpenseCategoryTypeController } from './expense-category-type.controller';
import { ExpenseCategoryTypeService } from './expense-category-type.service';

describe('ExpenseCategoryTypeController', () => {
  let controller: ExpenseCategoryTypeController;
  let service: ExpenseCategoryTypeService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ExpenseCategoryTypeController],
      providers: [{
        provide: ExpenseCategoryTypeService,
        useValue: {
          findAll: jest.fn(),
          findOne: jest.fn(),
          create: jest.fn(),
          update: jest.fn(),
          remove: jest.fn(),
        },
      }],
    }).compile();

    controller = module.get<ExpenseCategoryTypeController>(
      ExpenseCategoryTypeController,
    );
    service = module.get<ExpenseCategoryTypeService>(
      ExpenseCategoryTypeService,
    );
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
    expect(controller).toBeDefined();
  });
});