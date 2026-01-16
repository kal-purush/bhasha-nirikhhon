///<reference path="game.ts"/>


class Circle extends Game{

    constructor() {
      super();
      this.color = this.colorArray[Math.floor(Math.random() * this.colorArray.length)];
    }

    public update() {
        if (this.x + this.radius > innerWidth ||
            this.x - this.radius < 0) {
            this.dx = -this.dx;
        }

        if (this.y + this.radius > innerHeight ||
            this.y - this.radius < 0) {
            this.dy = -this.dy;
        }

        this.x += this.dx;
        this.y += this.dy;


        // interactivity waarbij de muis hover ervoor zorgt dat de cirkeltjes groter en kleiner worden.

        if (this.mouseX - this.x < 50 && this.mouse.x - this.x > -50 &&
            this.mouseY - this.y < 50 && this.mouse.y - this.y > -50
        ) {
            if (this.radius < this.maxRadius) {
                this.radius += 1;
            }

        } else if (this.radius > this.minRadius) {
            this.radius -= 1;
        }

        this.draw();

        requestAnimationFrame(() => this.update());
    }

    public draw() {
        this.context.beginPath();
        this.context.arc(this.x, this.y, this.radius, 0, Math.PI * 2, false);
        this.context.fillStyle = this.color;
        this.context.fill();
    }
}