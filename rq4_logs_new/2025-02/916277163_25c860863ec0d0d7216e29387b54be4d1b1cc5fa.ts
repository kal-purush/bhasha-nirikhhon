import { Test, TestingModule } from '@nestjs/testing';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';

import { SupplierTypeService } from '../supplier-type/supplier-type.service';

import { SupplierCategoryController } from './supplier-category.controller';
import { SupplierCategoryService } from './supplier-category.service';

describe('SupplierCategoryController', () => {
  let controller: SupplierCategoryController;
  let service: SupplierCategoryService;
  let supplierTypeService: SupplierTypeService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [SupplierCategoryController],
      providers: [
        {
          provide: SupplierCategoryService,
          useValue: {
            findAll: jest.fn(),
          },
        },
        {
          provide: SupplierTypeService,
          useValue: {
            findOne: jest.fn(),
          },
        },
      ],
    }).compile();

    controller = module.get<SupplierCategoryController>(
      SupplierCategoryController,
    );
    service = module.get<SupplierCategoryService>(SupplierCategoryService);
    supplierTypeService = module.get<SupplierTypeService>(SupplierTypeService);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
    expect(service).toBeDefined();
    expect(supplierTypeService).toBeDefined();
  });
});