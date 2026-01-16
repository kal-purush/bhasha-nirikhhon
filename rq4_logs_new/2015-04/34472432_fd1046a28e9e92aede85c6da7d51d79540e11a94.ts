module entities {

  export class Worm extends Phaser.Sprite {
    target:Phaser.Sprite;
    speed:number = 100;

    constructor(game:Phaser.Game) {
      super(game, 0, 0, 'worm-walk');

      this.anchor.setTo(0.5);
      this.animations.add('run');

      game.physics.enable(this, Phaser.Physics.ARCADE);
    }

    update() {
      super.update();

      var keyboard:Phaser.Keyboard = this.game.input.keyboard;
      var vel:Phaser.Point = new Phaser.Point();

      if (keyboard.isDown(Phaser.Keyboard.LEFT)) {
        this.scale.x = 1;
        vel.x -= this.speed;
      } else if (keyboard.isDown(Phaser.Keyboard.RIGHT)) {
        this.scale.x = -1;
        vel.x += this.speed;
      }
      if (keyboard.isDown(Phaser.Keyboard.UP)) {
        vel.y -= this.speed;
      } else if (keyboard.isDown(Phaser.Keyboard.DOWN)) {
        vel.y += this.speed;
      }
      this.body.velocity = vel;
      if (vel.isZero()) {
        this.animations.stop();
      } else {
        this.animations.play('run', 30, true);
      }
    }
  }
}