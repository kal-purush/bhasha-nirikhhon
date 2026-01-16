import Character, { Player, Enemy } from "./character";
import Action, { Actions, TargetType } from "./action";
import Narrator from "./narrator";

class Game {
  readonly player: Player;
  readonly enemy: Enemy;
  readonly narrator: Narrator;

  private lastUpdateTime: number;

  constructor() {
    this.lastUpdateTime = performance.now();
    this.player = new Player(this);
    this.enemy = new Enemy(this);
    this.narrator = new Narrator();
  }

  start() {
    // ゲームループを開始する
    this.update(performance.now());
  }

  private update(currentTime: number) {
    const deltaTime = currentTime - this.lastUpdateTime;
    const desiredFPS = 60;
    const frameInterval = 1000 / desiredFPS;

    if (deltaTime >= frameInterval) {
      // Update game state here
      this.player.process(deltaTime);
      this.enemy.process(deltaTime);

      this.lastUpdateTime = currentTime;
    }

    requestAnimationFrame(this.update.bind(this));
  }

  /**
   * アクションをトリガーする関数
   * 
   * @param action 発火させるアクション
   */
  triggerAction(action: Action<Actions>) {
    const targets = this.getTargets(action.character, action.targetType);
    for (const target of targets) {
      action.execute(target);
    }
  }

  /**
   * ターゲットを計算して返す関数
   * @param character ターゲットを取得しようとしているキャラクター
   */
  getTargets(character: Character, targetType: TargetType) {
    const targets: Array<Character> = [];
    switch (targetType) {
    case TargetType.self:
    case TargetType.both:
      targets.push(character);
    }
    switch (targetType) {
    case TargetType.opponent:
    case TargetType.both:
      targets.push(this.getOpponent(character));
    }
    return targets;
  }

  /**
   * 相手を取得する関数
   * この関数は、プレイヤーと敵が初期化されてから呼びだす必要がある
   * 
   * @param character 行動するキャラ
   * @returns 相手のキャラ
   */
  getOpponent(character: Character) {
    if (character == this.player) {
      return this.enemy;
    } else {
      return this.player;
    }
  }

}

export default Game;