import Chunk = require("./Chunk");

export class Character extends Phaser.Sprite {
    ground: Array<Chunk.Chunk>;
    lastTouch: number;
    speed: number;
    type: number;

    public constructor(game: Phaser.Game, x: number, y: number, type: number, ground: Array<Chunk.Chunk>) {
        super(game, x, y, 'place1');
        this.type = type;
        this.anchor = new Phaser.Point(0.5, 0.5);
        game.add.existing(this);
        game.physics.arcade.enable(this);

        this.body.drag.setTo(600, 0);
        this.body.maxVelocity.setTo(400, 5000);
        this.body.collideWorldBounds = false;
        this.lastTouch = 0;
        this.ground = ground;
    }

    jump() {
        if(this.body.onFloor() || this.body.touching.down) {
            this.lastTouch = this.game.time.time;
        }
        if(Date.now() - this.lastTouch < 100) {
            this.body.velocity.y -= 150;
        }
    }

    update() {
        super.update();
        this.body.acceleration.x += this.speed * 8;
        this.game.physics.arcade.collide(this, this.ground, null, null, this);
    }
}