class Tank extends RobotronSprite
{    
    constructor(game: Game, left: number, top: number) {
        super('tank', game, left, top, "all", Tank.cells);
        this.speed = 50;
        this.width = Tank.cells['all'][0].w * 2;
        this.height = Tank.cells['all'][0].h * 2;
        this.enemy = true;
        this.mustKill = true;
        this.canKill = true;
        this.score = 200;
        this.setRandomDirection();
    }

    mover(context: CanvasRenderingContext2D, time: number) {
        this.advanceFrame(time, 200);
        if (Math.random() < .005) this.setRandomDirection();
        this.move(time);
    }

    // override
    setRandomDirection() {
        var theta = Quark.random45degreeAngle();
        this.velocityX = Math.cos(theta) * this.speed;
        this.velocityY = Math.sin(theta) * this.speed;
    }

    kill(bullet: Bullet) {
        var horizontal = Math.abs(bullet.velocityY) > Math.abs(bullet.velocityX);
        var explosion = new Explosion(bullet.game, this.left, this.top, this.width, this.height, horizontal);
        bullet.game.playSound("sound_kill");
        this.game.removeSprite(this);
    }

    private static cells: ISpriteCells = {
        all: [
            { x: 1, y: 275, w: 26, h: 32 },
            { x: 1 + 38, y: 275, w: 26, h: 32 },
            { x: 1 + 38*2, y: 275, w: 26, h: 32 },
            { x: 1 + 38*3, y: 275, w: 26, h: 32 }
            ]
    }
}