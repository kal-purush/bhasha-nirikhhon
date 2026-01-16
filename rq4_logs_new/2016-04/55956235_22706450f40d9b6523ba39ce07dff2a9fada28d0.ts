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
}


/* main */
function main() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    for (let i = 0; i < 6; i++) {
        for (let j = 0; j < 6; j++) {
            let rgb: Object = {
                r: Math.floor(255 - 42.5 * i),
                g: Math.floor(255 - 42.5 * j),
                b: 0
            }
            drawFactory
                .fillStyle(`rgb(${rgb.r}, ${rgb.g}, ${rgb.b})`)
                .fillRect(j * 25, i * 25, 25, 25);
        }
    }
}
main()