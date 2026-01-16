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
    public style: () => string;
    constructor(rect: IRect, style?: IStyle) {
        this._rect = rect;
        if (style) {
            this._style = style;
            this.style = function(): string {
                let {r, g, b, a} = this._style;
                let strStyle = a ?
                    `rgba(${r}, ${g}, ${b}, ${a})` :
                    `rgb(${r}, ${g}, ${b})`;
                return strStyle;
            }
        }
    }
    get rect(): IRect {
        return this._rect;
    }
}


/* DrawFactory */
class DrawFactory {
    private _ctx: CanvasRenderingContext2D;
    constructor(canvas: HTMLCanvasElement) {
        this._ctx = canvas.getContext("2d");
    }
    fillRect(Rect: Rect) {
        let style = Rect.style;
        let {x, y, w, h} = Rect.rect;

        if (style) {
            this._ctx.fillStyle = style;
        }
        this._ctx.fillRect(x, y, w, h);
    }
    clearRect(Rect: Rect) {
        let style = Rect.style;
        let {x, y, w, h} = Rect.rect;

        if (style) {
            this._ctx.fillStyle = style;
        }
        this._ctx.clearRect(x, y, w, h);
    }
    strokeRect(Rect: Rect) {
        let style = Rect.style;
        let {x, y, w, h} = Rect.rect;

        if (style) {
            this._ctx.fillStyle = style;
        }
        this._ctx.strokeRect(x, y, w, h);
    }
}


/* main */
function main() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    let rects = {
        solidRect: new Rect({ x: 25, y: 25, w: 100, h: 100 }),
        clarityRect: new Rect({ x: 45, y: 45, w: 60, h: 60 }),
        boderRect: new Rect({ x: 50, y: 50, w: 50, h: 50 }),
    };
    drawFactory.fillRect(rects.solidRect)
    drawFactory.clearRect(rects.clarityRect)
    drawFactory.strokeRect(rects.boderRect)
}
main()