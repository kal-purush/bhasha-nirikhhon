/// <reference path="BaseBehavior.ts" />

class FireableBehavior extends BaseBehavior {

    game: Phaser.Game;
    name: string = "Fireable";

    bullets: Phaser.Group;
    fireRate: number = 1000;
    nextFire: number = 0;

    constructor(game: Phaser.Game) {
        super();

        this.game = game;
        this.bullets = this.game.add.group();
        this.bullets.enableBody = true;
        this.bullets.physicsBodyType = Phaser.Physics.ARCADE;
        this.bullets.createMultiple(30, 'bullet', 0, false);
        this.bullets.setAll('anchor.x', 0.5);
        this.bullets.setAll('anchor.y', 0.5);
        this.bullets.setAll('outOfBoundsKill', true);
        this.bullets.setAll('checkWorldBounds', true);
    }

    update(): void {
        // console.log("[Fireable] :: update");
        if (this.game.input.activePointer.isDown)
        {
            this.fire();
        }
    }

    fire(): void {
        if (this.game.time.now > this.nextFire && this.bullets.countDead() > 0)
        {
            console.log('fire');
            this.nextFire = this.game.time.now + this.fireRate;

            var bullet = this.bullets.getFirstExists(false);

            bullet.reset(this.target.turret.x, this.target.turret.y);

            bullet.rotation = this.game.physics.arcade.moveToPointer(bullet, 1000, this.game.input.activePointer, 500);
        }
    }
}