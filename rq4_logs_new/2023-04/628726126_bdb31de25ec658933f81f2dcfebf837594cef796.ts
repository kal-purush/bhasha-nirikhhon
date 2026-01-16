import { Test, TestingModule } from '@nestjs/testing';
import { LiveChatController } from './liveChat.controller';

describe('AuthController', () => {
    let controller: LiveChatController;

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            controllers: [LiveChatController],
        }).compile();

        controller = module.get<LiveChatController>(LiveChatController);
    });

    it('should be defined', () => {
        expect(controller).toBeDefined();
    });
});