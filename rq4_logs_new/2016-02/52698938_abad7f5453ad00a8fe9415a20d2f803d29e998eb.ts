
import Bullet = require('./Bullet');
import Enemy = require('./Enemy');
import PlayState = require('../states/PlayState');

class EnemyBullet extends Bullet {

  constructor(state: PlayState, enemy: Enemy) {
    super(state);
    this.setShooter(enemy);
  }

  reset() {
    super.reset();

    this.damage = 1;
    this.velocity.set(0, EnemyBullet.SPEED);
  }

  setShooter(enemy: Enemy) {
    super.setShooter(enemy);

    this.x = enemy.x;
    this.y = enemy.y + 50;
  }



}

export = EnemyBullet;