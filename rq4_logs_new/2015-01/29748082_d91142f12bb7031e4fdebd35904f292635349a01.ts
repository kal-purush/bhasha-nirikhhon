import Chunk = require("./Chunk");

export class Monster extends Phaser.Sprite {
    type: number;
    ground: Array<Chunk.Chunk>;
    hp: number;
    maxhp: number = 3;
    constructor(game: Phaser.Game, x: number, y: number, type: number) {
        super(game, x, y, "place4");
        this.type = type;
        this.hp = this.maxhp;
        game.add.existing(this);
        game.physics.arcade.enable(this);

        this.body.drag.setTo(600, 0);
        this.body.maxVelocity.setTo(400, 5000);
    }

    update() {
        this.game.physics.arcade.collide(this, this.ground, null, null, this);
        if(this.body.onFloor() || this.body.touching.down) {
            this.body.velocity.y -= 800;
        }
    }

    hit() {
        this.hp--;
        var tween = this.game.add.tween(this).to({ alpha: (this.hp/this.maxhp) }, 100, Phaser.Easing.Linear.None, true);
        if(this.hp <= 0) {
            this.kill();
        }
    }
}