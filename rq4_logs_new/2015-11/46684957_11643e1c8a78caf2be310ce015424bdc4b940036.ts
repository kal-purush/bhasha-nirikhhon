module t8 {
    var H: Grix

    export function setup() {
        H = new Grix()
            .rect(15, 100)
            .rect(50, 15, 15, 40)
            .rect(15, 100, 65, 0)
            .rect(15, 70, 95, 0)
            .rect(15, 15, 95, 85)
            .setColorRGB(255, 150, 90)
            .populate();
    }
    export function update(delta: number) { }
    export function render(delta: number) {
        H.setPivotMove(.5, .5)
        H.scaleWidthToSize(250)
        H.rotateDeg(135)
        H.moveTo(250, 250)
        H.render();
        H.clean();

        H.moveTo(20, 20);
        H.render();
        H.setPivotMove(1, 0);
        H.moveTo(Plena.width - 20, 20)
        H.render()
        H.setPivotMove(0, 1);
        H.moveTo(20, Plena.height - 20);
        H.render();
        H.setPivotMove(1, 1);
        H.moveTo(Plena.width - 20, Plena.height - 20)
        H.render()
    }
}

Plena.init(t8.setup, t8.render, t8.update, 500, 500, [.15, .15, .15, 1]);