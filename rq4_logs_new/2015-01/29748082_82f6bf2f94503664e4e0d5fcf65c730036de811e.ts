export class Player extends Phaser.Sprite {
    constructor(game: Phaser.Game, x: number, y: number) {
        super(game, x, y, 'place1');
        game.add.existing(this);
        console.log(this);
    }

    update() {
        this.body.velocity.x *= 0.95;
        this.body.collideWorldBounds = true;

        if(this.game.input.keyboard.isDown(Phaser.Keyboard.LEFT)) {
            this.body.velocity.x += - 150;
        } else if(this.game.input.keyboard.isDown(Phaser.Keyboard.RIGHT)) {
            this.body.velocity.x += 150;
        } else if(this.game.input.keyboard.isDown(Phaser.Keyboard.UP)) {
            this.body.velocity.y -= 50;
        }
    }
}