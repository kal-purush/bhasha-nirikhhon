import { Test, TestingModule } from '@nestjs/testing';
import { TimeSlotController } from './time-slot.controller';

describe('TimeSlotController', () => {
  let controller: TimeSlotController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [TimeSlotController],
    }).compile();

    controller = module.get<TimeSlotController>(TimeSlotController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});