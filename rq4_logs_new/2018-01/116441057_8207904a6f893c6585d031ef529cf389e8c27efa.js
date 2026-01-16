function Grass(x) {

    this.lgt = random(20);
    this.ini = random(30);
    this.x = x;
    this.y;
    this.velocity = 1;

    this.draw = function() {
        stroke(0, 0, 0);
        line(this.x, height - 1 - this.ini, this.x, height - this.ini - this.lgt);
    }

    this.update = function() {
        this.x -= this.velocity;
    }

    this.offscreen = function() {
        if (this.x < 0) {
            return true;
        } else {
            return false;
        }
    }
}