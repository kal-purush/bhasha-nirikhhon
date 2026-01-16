import { IGame } from "@turbo-hearts-scores/shared";
import { GameLoader } from "./gameLoader";
import { Api } from "./transport";

export class PlayerVsGameLoader implements GameLoader {
  private api: Api;
  private playerId: string;
  private playerId2: string;

  constructor(api: Api, playerId: string, playerId2: string) {
    this.api = api;
    this.playerId = playerId;
    this.playerId2 = playerId2;
  }

  public loadGames(): Promise<IGame[]> {
    return this.api.fetchPlayeVsGames(this.playerId, this.playerId2);
  }
}