import { Err, Ok, Result } from "ts-results";
import { singleton } from "tsyringe";

import { IPlayerRepository } from "./../interface/playerRepository";

import { Player, PlayerId } from "@/domain/entity/player";
import { RoomId } from "@/domain/entity/room";
import { DataNotFoundError, RepositoryError } from "@/error/repository";
import { OkOrErr } from "@/misc/result";
import { voidType } from "@/misc/type";

const store: Player[] = [];

@singleton()
export class InMemoryPlayerRepository implements IPlayerRepository {
  findById(id: PlayerId): Result<Player, RepositoryError> {
    return OkOrErr(
      store.find((player) => player.id === id),
      new DataNotFoundError(),
    );
  }
  findByRoomId(roomId: RoomId): Result<Player[], RepositoryError> {
    return OkOrErr(
      store.filter((player) => player.roomId === roomId),
      new DataNotFoundError(),
    );
  }
  save(player: Player): Result<Player, RepositoryError> {
    const index = store.findIndex((p) => p.id === player.id);
    if (index !== -1) {
      store[index] = player;

      return Ok(store[index]);
    }

    const newIndex = store.push(player);

    return Ok(store[newIndex]);
  }
  delete(id: PlayerId): Result<void, RepositoryError> {
    const index = store.findIndex((p) => p.id === id);
    if (index === -1) {
      return Err(new DataNotFoundError());
    }

    store.splice(index, 1);

    return Ok(voidType);
  }
}