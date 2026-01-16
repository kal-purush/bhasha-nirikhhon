/* CanvasApp */
class CanvasApp {
    private _body: HTMLElement = document.body;
    private _canvas: HTMLCanvasElement = document.createElement('canvas');
    constructor(w: number = 300, h: number = 150) {
        // Initial html
        this._body.appendChild(this._canvas);
        this._canvas.width = w;
        this._canvas.height = h;
    }
    get canvas(): HTMLCanvasElement {
        return this._canvas;
    }
}


/* DrawFactory */
class DrawFactory {
    private _ctx: CanvasRenderingContext2D;
    constructor(canvas: HTMLCanvasElement) {
        this._ctx = canvas.getContext("2d");
    }
    beginPath(): DrawFactory {
        this._ctx.beginPath();
        return this;
    }
    moveTo(x: number, y: number): DrawFactory {
        this._ctx.moveTo(x, y);
        return this;
    }
    lineTo(x: number, y: number): DrawFactory {
        this._ctx.lineTo(x, y);
        return this;
    }
    fill(): DrawFactory {
        this._ctx.fill();
        return this;
    }
    arc(x: number, y: number, radius: number, startAngle: number, endAngle: number, anticlockwise?: boolean): DrawFactory {
        this._ctx.arc(x, y, radius, startAngle, endAngle, anticlockwise);
        return this;
    }
    stroke() {
        this._ctx.stroke();
        return this;
    }
}


/* main */
function main() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    drawFactory
        .beginPath()
        .arc(75, 75, 50, 0, Math.PI * 2, true)
        .moveTo(110, 75)
        .arc(75, 75, 35, 0, Math.PI, false)
        .moveTo(65, 65)
        .arc(60, 65, 5, 0, Math.PI * 2, true)
        .moveTo(95, 65)
        .arc(90, 65, 5, 0, Math.PI * 2, true)
        .stroke();
}
main()