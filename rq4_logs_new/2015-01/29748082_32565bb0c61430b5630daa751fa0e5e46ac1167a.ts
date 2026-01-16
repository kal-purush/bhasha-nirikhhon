export class Bullet extends Phaser.Sprite {
    speed: number = 1600;
    constructor(game: Phaser.Game, x: number, y:number) {
        super(game, x, y, 'place3');
        game.add.existing(this);
        game.physics.arcade.enable(this);
        this.anchor.set(0.5, 0.5);
        this.body.maxVelocity.setTo(this.speed, 400);
        this.body.velocity.setTo(this.speed, 0);
        this.checkWorldBounds = true;
        this.outOfBoundsKill = true;
        this.body.allowGravity = false;
    }
    update() {
        this.body.velocity.y += Math.sin(this.game.time.time) * 30;
    }
}