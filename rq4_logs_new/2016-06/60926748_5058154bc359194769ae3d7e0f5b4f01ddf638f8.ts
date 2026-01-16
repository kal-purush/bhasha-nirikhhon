export class Player extends Phaser.Sprite {
  static maxVelocity = 1000;
  static acceleration = Player.maxVelocity * 0.2;
  static jumpSpeed = 2000;
  static gravity = 4000;

  constructor(game: Phaser.Game, x: number, y: number, spriteKey: string) {
    super(game, x, y, spriteKey);

    this.scale.set(game.height * 0.1 / this.height);
    this.anchor.set(0.5);

    game.add.existing(this);
    game.physics.arcade.enable(this);
    this.body.gravity.y = Player.gravity;
    this.body.collideWorldBounds = true;
  }

  accelerateTo(finalVelocity: number) {
    if (finalVelocity < 0) { // Accelerate left
      if (-Player.maxVelocity < this.body.velocity.x) {
        this.body.velocity.x -= Player.acceleration;
      }
    }
    else if (0 < finalVelocity) { // Accelerate right
      if (this.body.velocity.x < Player.maxVelocity) {
        this.body.velocity.x += Player.acceleration;
      }
    }
    else { // Stop movement
      if (this.body.velocity.x < 0) {
        this.body.velocity.x += Player.acceleration;
      }
      else if (0 < this.body.velocity.x) {
        this.body.velocity.x -= Player.acceleration;
      }
    }
  }
}