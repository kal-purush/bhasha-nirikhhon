import { Err, Ok, Result } from "ts-results";

import { PlayerId } from "@/domain/entity/player";
import { PlayerRepository } from "@/domain/repository/playerRepository";
import { UseCaseError } from "@/error/usecase";
import { PlayerNotFoundError } from "@/error/usecase/player";
import { RepositoryOperationError } from "@/error/usecase/room";

export const DeletePlayerUseCase =
  // prettier-ignore
  (playerRepository: PlayerRepository) =>
    (id: PlayerId): Result<void, UseCaseError> => {
    // 該当のプレイヤーが存在しないなら削除できない
      const playerResult = playerRepository.findById(id);
      if (playerResult.err) {
        return Err(new PlayerNotFoundError());
      }
      const player = playerResult.unwrap();

      // プレイヤーを削除する
      const playerRepoResult = playerRepository.delete(player.id);
      if (playerRepoResult.err) {
        return Err(new RepositoryOperationError(playerRepoResult.val));
      }

      return Ok(playerRepoResult.unwrap());
    };