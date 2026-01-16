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
export class Rect {
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