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
}


/* main */
function main() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    let squares = {
        yellowSquare: {
            style: '#FD0',
            rect: [0, 0, 75, 75],
        },
        greenSquare: {
            style: '#6C0',
            rect: [75, 0, 75, 75],
        },
        blueSquare: {
            style: '#09F',
            rect: [0, 75, 75, 75],
        },
        redSquare: {
            style: '#F30',
            rect: [75, 75, 150, 150],
        },

    }
    for (let square in squares) {
        let theSquare = squares[square];
        let [x, y, w, h] = theSquare.rect
        drawFactory
            .fillStyle(theSquare.style)
            .fillRect(x, y, w, h);
    }


    // set transparency value
    drawFactory.ctx.fillStyle = '#FFF';
    drawFactory.ctx.globalAlpha = 0.2;

    // Draw semi transparent circles
    for (let i = 0; i < 7; i++) {
        drawFactory
            .beginPath()
            .arc(75, 75, 10 + 10 * i, 0, Math.PI * 2, true)
            .fill()
    }

}
main()