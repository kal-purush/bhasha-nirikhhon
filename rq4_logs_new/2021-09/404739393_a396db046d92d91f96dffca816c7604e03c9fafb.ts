import { Test, TestingModule } from '@nestjs/testing';
import { GameRepository } from './game.repository';
import { GameService } from './game.service';

describe('GameService', () => {
  let gameService: GameService;
  let gameRepository: GameRepository;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      providers: [GameService, GameRepository],
    }).compile();

    gameRepository = app.get<GameRepository>(GameRepository);
    gameService = app.get<GameService>(GameService);
  });

  it('game map test', () => {
    const room = gameRepository.createGameRoom();
    expect(gameRepository.gameRoomMap.size).toBe(1);
    const roomId: number = room.socketRoomId;
    const tmp = gameRepository.getGameRoom(roomId);

    expect(room).toBe(tmp);
  });
});