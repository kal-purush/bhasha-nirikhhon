import { Test, TestingModule } from '@nestjs/testing';
import { CustomerController } from './customer.controller';
import { CustomerService } from './customer.service';

const MockCustomerService = jest.fn<Partial<CustomerService>, []>(() => ({
  create: jest.fn(),
  findAll: jest.fn(),
  findOne: jest.fn(),
  update: jest.fn(),
  remove: jest.fn(),
}));

describe('CustomerController', () => {
  let controller: CustomerController;
  let customerService: CustomerService;

  beforeEach(async () => {
    customerService = MockCustomerService() as CustomerService;
    const module: TestingModule = await Test.createTestingModule({
      controllers: [CustomerController],
      providers: [{ provide: CustomerService, useValue: customerService }],
    }).compile();

    controller = module.get<CustomerController>(CustomerController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});