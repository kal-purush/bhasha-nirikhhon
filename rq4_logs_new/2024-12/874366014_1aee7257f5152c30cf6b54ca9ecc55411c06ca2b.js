class Bolt {
    constructor(x, y, width, height) {
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }

    update(speed) {
        this.y += speed
    }

    draw() {
        c.drawImage(loadedTextures["bolt"], this.x, this.y)
    }
}