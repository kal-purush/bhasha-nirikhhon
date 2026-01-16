import { Player } from './player';

export class Aim extends Phaser.Sprite {
  constructor(player: Player) {
    super(player.game, 0, 0, 'platform');

    this.scale.set(1000, 30);
    this.anchor.set(0, 0.5);
    this.rotation = -45;

    player.addChild(this);
  }
}