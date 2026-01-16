module t6 {
    var bat: Grix
    var ball: Grix;

    var x1 = window.innerHeight / 2;
    var x2 = window.innerHeight / 2;

    var xB = window.innerWidth / 2;
    var yB = window.innerHeight / 2;
    var v: number;
    var vm = 10;
    var direction: number;
    var radius = 20;

    var isPlaying = false;

    export function setup() {
        bat = new Grix().colorRGB(160, 160, 240).rect(20, 120).populate();
        ball = new Grix().circle(radius).colorRGB(240, 160, 160).populate();

        Keyboard.addPressedEvent(startBall, Keyboard.KEY_SPACE);
    }

    export function update(delta: number) {
        if (Keyboard.isKeyDown(Keyboard.KEY_W)) x1 -= vm;
        if (Keyboard.isKeyDown(Keyboard.KEY_S)) x1 += vm;
        if (Keyboard.isKeyDown(Keyboard.KEY_UP)) x2 -= vm;
        if (Keyboard.isKeyDown(Keyboard.KEY_DOWN)) x2 += vm;

        if (x1 < 60) x1 = 60;
        if (x1 > Plena.height - 60) x1 = Plena.height - 60;
        if (x2 < 60) x2 = 60;
        if (x2 > Plena.height - 60) x2 = Plena.height - 60;

        if (v > 0) {
            xB += v * Math.cos(direction);
            yB += v * Math.sin(direction);

            if (yB - radius <= 0 || yB + radius >= Plena.height) direction = Math.PI * 2 - direction;
        }
    }

    export function render(delta: number) {
        GLF.clearColor([.2, .2, .2, 1])  

        bat.setPivotMove(0.5, 0.5)
        bat.moveTo(25, x1)
        bat.render();
        bat.moveTo(window.innerWidth - 25, x2);
        bat.render();

        ball.setPivotMove(0.5, 0.5)
        ball.moveTo(xB, yB)
        ball.render()
    }

    function startBall() {
        if (!isPlaying) {
            v = 5;
            direction = Math.random() * Math.PI * 2;
            isPlaying = true;
        }
    }
}

Plena.init(t6.setup, t6.render, t6.update);