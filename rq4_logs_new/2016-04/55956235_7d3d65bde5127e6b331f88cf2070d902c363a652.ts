/* CanvasApp */
class CanvasApp {
    private _body: HTMLElement = document.body;
    private _canvas: HTMLCanvasElement = document.createElement('canvas');
    constructor(w: number = 300, h:number = 150) {
        // Initial html
        this._body.appendChild(this._canvas);
        this._canvas.width = w;
        this._canvas.height = h;
    }
    get canvas(): HTMLCanvasElement {
        return this._canvas;
    }
}


/* Rect */
interface IRect {
    x: number;
    y: number;
    w: number;
    h: number;
}
interface IStyle {
    r: number;
    g: number;
    b: number;
    a?: number;
}
class Rect {
    private _rect: IRect;
    private _style: IStyle;
    constructor(rect: IRect, style: IStyle) {
        this._rect = rect;
        this._style = style;
    }
    get rect():IRect {
        return this._rect;
    }
    get style():string {
        let {r, g, b, a} = this._style;
        let strStyle = a ?
            `rgba(${r}, ${g}, ${b}, ${a})` :
            `rgb(${r}, ${g}, ${b})`;
        return strStyle;
    }
}


/* DrawFactory */
class DrawFactory {
    private _ctx: CanvasRenderingContext2D;
    constructor(canvas: HTMLCanvasElement) {
        this._ctx = canvas.getContext("2d");
    }
    drawRect(Rect: Rect) {
        let parsedStyle = Rect.style;
        let {x, y, w, h} = Rect.rect;

        this._ctx.fillStyle = parsedStyle;
        this._ctx.fillRect(x, y, w, h);
    }
}


/* main */
function main() {
    let canvasApp = new CanvasApp(150,150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    let rects = {
        redRect: new Rect(
            { x: 10, y: 10, w: 55, h: 50 },
            { r: 200, g: 0, b: 0 }
        ),
        blueRect: new Rect(
            { x: 30, y: 30, w: 55, h: 50 },
            { r: 0, g: 0, b: 200, a: 0.5 }
        )
    };

    drawFactory.drawRect(rects.redRect);
    drawFactory.drawRect(rects.blueRect);
}
main()