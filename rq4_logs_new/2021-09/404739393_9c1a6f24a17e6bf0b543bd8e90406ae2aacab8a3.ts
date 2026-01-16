import { Injectable } from '@nestjs/common';
import { GameRepository } from './game.repository';

@Injectable()
export class GameRoomService {
  constructor(private gameRepository: GameRepository) {}

  invite(userId: number, targetUserId: number): number {
    const gameRoom = this.gameRepository.createGameRoom();

    gameRoom.player1 = { id: userId, isAccept: true };
    gameRoom.player2 = { id: targetUserId, isAccept: false };
    return gameRoom.socketRoomId;
  }

  accept(userId: number, roomId: number) {
    const gameRoom = this.gameRepository.getGameRoom(roomId);

    if (gameRoom.player1.id == userId) gameRoom.player1.isAccept = true;
    if (gameRoom.player2.id == userId) gameRoom.player2.isAccept = true;
    if (gameRoom.player1.isAccept && gameRoom.player2.isAccept) return gameRoom;
    return;
  }

  cancel(userId: number, roomId: number) {
    const gameRoom = this.gameRepository.getGameRoom(roomId);

    let canceledPlayerId;
    if (gameRoom.player1.id == userId) canceledPlayerId = gameRoom.player2.id;
    else if (gameRoom.player2.id == userId)
      canceledPlayerId = gameRoom.player1.id;
    else return;
    this.gameRepository.deleteGameRoom(roomId);
    return canceledPlayerId;
  }
}