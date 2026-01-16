import Character, { Player, Enemy } from "./character";
import Game from "./game";

class CharacterManager {
  public group: Array<Character>;
  
  constructor() {
    this.group = [];
  }

  process(deltaTime: number) {
    for (const character of this.group) {
      character.process(deltaTime);
    }
  }

  getTargetedCharacter() {
    return this.group[0];
  }
}


class PlayerManager extends CharacterManager {
  public readonly group: Array<Player>;

  constructor(game: Game) {
    super();
    this.group = [new Player(game, this, {
      name: "プレイヤー",
    })];
  }

  get totalHp() {
    let total = 0;
    for (const character of this.group) {
      total += character.status.hp;
    }
    return total;
  }

  get totalMp() {
    let total = 0;
    for (const character of this.group) {
      total += character.status.mp;
    }
    return total;
  }

  get totalMaxHp() {
    let total = 0;
    for (const character of this.group) {
      total += character.maxHp;
    }
    return total;
  }

  get totalMaxMp() {
    let total = 0;
    for (const character of this.group) {
      total += character.maxMp;
    }
    return total;
  }
}

class EnemyManager extends CharacterManager {
  public readonly group: Array<Enemy>;
  
  constructor(game: Game) {
    super();
    this.group = [new Enemy(game, this, {
      name: "敵",
    })];
  }
}

export { PlayerManager, EnemyManager };
export default CharacterManager;