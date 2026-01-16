class RobotronSprite extends AnimatedSprite {
    canKill: boolean = false;
    mustKill: boolean = false;
    score: number = 0;
    constructor(name: string, game: Game, left: number, top: number, startDirection?: string, cells?: ISpriteCells) {
        super(name, game, left, top, startDirection, cells);
    }

    setRandomDirection() {
        var rand = Math.random() * 4;

        this.velocityX = this.velocityY = 0;

        if (rand < 1) {
            if (this.direction != 'all') this.direction = 'left';
            this.velocityX = -this.speed;
        } else if (rand < 2) {
            if (this.direction != 'all') this.direction = 'right';
            this.velocityX = this.speed;
        } else if (rand < 3) {
            if (this.direction != 'all') this.direction = 'up';
            this.velocityY = -this.speed;
        } else {
            if (this.direction != 'all') this.direction = 'down';
            this.velocityY = this.speed;
        }
    }

    // default movement - bounce off of walls
    adjustMoveDelta(deltaX: number, deltaY: number) {        
        if (this.left + this.width + deltaX > this.game.right) {
            if (this.direction != 'all') this.direction = 'left';
            this.velocityX = -this.speed;
            deltaX = 0;
        } else if (this.left + deltaX < this.game.left) {
            if (this.direction != 'all') this.direction = 'right';
            this.velocityX = this.speed;
            deltaX = 0;
        }
        if (this.top + this.height + deltaY > this.game.bottom) {
            if (this.direction != 'all') this.direction = 'up';
            this.velocityY = -this.speed;
            deltaY = 0;
        } else if (this.top + deltaY < this.game.top) {
            if (this.direction != 'all') this.direction = 'down';
            this.velocityY = this.speed;
            deltaY = 0;
        }        
        return { deltaX: deltaX, deltaY: deltaY };
    }   

    kill(bullet: Bullet) {
    }
}