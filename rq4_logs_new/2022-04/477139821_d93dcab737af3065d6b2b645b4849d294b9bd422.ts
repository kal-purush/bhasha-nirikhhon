import { Test, TestingModule } from '@nestjs/testing';
import { BookFoodController } from './book-food.controller';

describe('BookFoodController', () => {
  let controller: BookFoodController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [BookFoodController],
    }).compile();

    controller = module.get<BookFoodController>(BookFoodController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});