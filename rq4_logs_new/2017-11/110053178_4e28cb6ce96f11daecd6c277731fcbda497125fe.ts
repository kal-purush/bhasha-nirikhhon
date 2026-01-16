import { Player } from './player';

export class Game {
  name: string;
  players: Array<Player>;

  constructor(name: string, players: Array<Player>) {
    this.name = name;
    this.players = players;
  }
}