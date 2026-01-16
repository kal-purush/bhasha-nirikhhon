"use strict";
class Ghoul extends Template {
    constructor(x, y, w, h, path) {
        super(x, y, w, h);
        this.ctx = canvas.getContext('2d');
        this.velocity = { x: 0, y: 0 };
        this.path = path;
        this.img = new Image();
        this.img.src = './img/spectre.png';
        this.source = { x: 0, y: 96, w: 96, h: 32 };
        this.currentFrame = 0;
        this.frameCount = 0;
    }
    draw() {
        ctx.drawImage(this.img, this.source.w * this.currentFrame / 3, this.source.y, this.source.w / 3, this.source.h, this.x, this.y, this.w, this.h);
    }
    animate() {
        this.frameCount++;
        if (this.frameCount % 7 === 0 && this.velocity.x !== 0 || this.frameCount % 5 === 0 && this.velocity.y !== 0)
            this.currentFrame++;
        if (this.currentFrame > 2)
            this.currentFrame = 0;
    }
    update() {
        this.draw();
        this.animate();
        if (this.x === this.path.x1 && this.y >= this.path.y1) {
            this.velocity.x = 0;
            this.velocity.y = 5;
            this.source = { x: 0, y: 0, w: 96, h: 32 };
            this.x = this.x + this.velocity.x;
            this.y = this.y + this.velocity.y;
        }
        if (this.x >= this.path.x2 && this.path.y2 === this.y) {
            this.velocity.y = 0;
            this.velocity.x = 5;
            this.source = { x: 0, y: 64, w: 96, h: 32 };
            this.x = this.x + this.velocity.x;
            this.y = this.y + this.velocity.y;
        }
        if (this.path.x3 === this.x && this.y <= this.path.y3) {
            this.velocity.x = 0;
            this.velocity.y = -5;
            this.source = { x: 0, y: 96, w: 96, h: 32 };
            this.x = this.x + this.velocity.x;
            this.y = this.y + this.velocity.y;
        }
        if (this.x <= this.path.x4 && this.path.y4 === this.y) {
            this.velocity.x = -5;
            this.velocity.y = 0;
            this.source = { x: 0, y: 32, w: 96, h: 32 };
            this.x = this.x + this.velocity.x;
            this.y = this.y + this.velocity.y;
        }
    }
}