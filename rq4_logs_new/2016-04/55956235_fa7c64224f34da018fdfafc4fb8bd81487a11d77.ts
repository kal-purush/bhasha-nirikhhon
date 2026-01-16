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
    get ctx(): CanvasRenderingContext2D {
        return this._ctx;
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
    stroke(): DrawFactory {
        this._ctx.stroke();
        return this;
    }
    closePath(): DrawFactory {
        this._ctx.closePath();
        return this;
    }
    fillStyle(style: string | CanvasGradient | CanvasPattern): DrawFactory {
        this._ctx.fillStyle = style;
        return this
    }
    fillRect(x: number, y: number, w: number, h: number): DrawFactory {
        this._ctx.fillRect(x, y, w, h)
        return this
    }
    strokeStyle(style: string | CanvasGradient | CanvasPattern): DrawFactory {
        this._ctx.strokeStyle = style;
        return this
    }
    lineWidth(w: number): DrawFactory {
        this._ctx.lineWidth = w
        return this;
    }
}


function draw() {
    var ctx = document.getElementById('canvas').getContext('2d');
    for (var i = 0; i < 10; i++) {
        ctx.lineWidth = 1 + i;
        ctx.beginPath();
        ctx.moveTo(5 + i * 14, 5);
        ctx.lineTo(5 + i * 14, 140);
        ctx.stroke();
    }
}
/* main */
function main() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    for (let i = 0; i < 10; i++) {
        drawFactory
            .lineWidth(1 + i)
            .beginPath()
            .moveTo(5 + i * 14, 5)
            .lineTo(5 + i * 14, 140)
            .stroke();
    }
}
main()