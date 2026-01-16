module t2 {
    var marbles: Grix;
    var count: number = 0;
    var BLACK = "black";
    var WHITE = "white"

    export function setup() {
        marbles = new Grix()
            .rect(100, 100)
            .addSprite(Plena.loadSpriteFile("marbles.jpg").addImgs([BLACK, WHITE], 0, 0, 100, 100, 2))
            .populate();
        Keyboard.addPressedEvent(Keyboard.KEY_SPACE, increseCount);
    }

    export function update() {

    }

    export function render() {
        for (var i = 0; i < count; i++) {
            marbles.moveTo((i % 5) * 100, 100 * Math.floor(i / 5));
            marbles.setActiveImg(Keyboard.isKeyDown(Keyboard.KEY_I) ^ (i & 1) ?  WHITE : BLACK);
            marbles.render();
        }
    }

    function increseCount() {
        if (count < 25) count++;
    }
}

Plena.init(t2.setup, t2.render, t2.update, 500, 500, [0.4, 0.8, 0.6, 1]);