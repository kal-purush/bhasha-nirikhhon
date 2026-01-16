import GroundTile = require("./GroundTile");

export class Ground extends Phaser.Group {
    pos: number;
    constructor(game: Phaser.Game) {
        super(game);
        for(var i = 0; i <= game.width; i += 64) {
            this.addNew(i);
        }
        this.pos = 0;
    }
    addNew(x: number) {
        this.add(new GroundTile.GroundTile(this.game, x, this.game.height - (128 * Math.random()) - 64));
    }
    update() {
        super.update();
        this.pos += 6;
        if(this.pos > 32) {
            this.addNew(this.game.width);
            this.pos = 0.5;
        }
    }
}