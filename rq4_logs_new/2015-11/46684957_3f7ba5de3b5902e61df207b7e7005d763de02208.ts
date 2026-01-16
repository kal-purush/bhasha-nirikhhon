module t3 {
    var cat:Grix;
    var animation = 0;
    var x = 0;
    var direction = 1;

    export function setup() {
        cat = new Grix()
            .animationFromSprite(Plena.loadSpriteFile("cats.png").addImgs("cat", 0, 5 * 32, 32, 32, 3))
            .populate();
        Keyboard.addPressedEvent(Keyboard.KEY_SPACE, switchMovement)
    }

    export function update() {
        animation += 0.1;
        x += direction * 2;

        if (x > 500) x = -cat.getWidth();
        if (x < -cat.getWidth()) x = 500;
    }

    export function render() {
        cat.setPivotMove(0, 0.5)
        cat.scaleToSize(100, 100)
        cat.moveTo(x, 250)
        cat.mirrorHorizontal(direction==1)
        cat.animationStep(Math.floor(animation))
        cat.render();
    }

    function switchMovement() {
        direction *= -1;
    }
}

Plena.init(t3.setup, t3.render, t3.update, 500, 500, [0.4, 0.8, 0.6, 1]);