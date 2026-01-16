import { Test, TestingModule } from '@nestjs/testing';
import { MessagingControllerCustom } from '../../../../apps/custom-two/src/messaging/messaging.controller';

describe('MessagingControllerCustom', () => {
  let controller: MessagingControllerCustom;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [MessagingControllerCustom],
    }).compile();

    controller = module.get<MessagingControllerCustom>(
      MessagingControllerCustom,
    );
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});