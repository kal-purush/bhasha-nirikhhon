/// <reference path="GameObject.ts" />
class Enemy extends GameObject {
    private colour = this.randomColour();

    constructor(canvas:HTMLCanvasElement) {
        super(canvas);
        this.width = canvas.width / 15;
        this.height = canvas.height / 20;
    }

    draw() {
        if (this.visible) {
            this.ctx.beginPath();
            this.ctx.rect(this.x, this.y, this.width, this.height);
            this.ctx.fillStyle = this.colour;
            this.ctx.fill();
            this.ctx.closePath();
        }
    }

    spawn() {
        this.active = true;
        this.visible = true;
        this.x = canvas.width;
        this.y = canvas.height * Math.random();
        this.colour = this.randomColour();
    }

    updatePosition(timeFactor) {
        if (this.active) {
            this.x -= 7 * timeFactor;
            if (this.x < 0) {
                this.active = false;
                this.visible = false;
            }
        }
    }
}