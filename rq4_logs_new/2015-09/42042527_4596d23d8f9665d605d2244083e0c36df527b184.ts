/**
 * Canvas辅助类
 */
class iPen {
    private _canvas: HTMLCanvasElement;
    private _cxt2d: CanvasRenderingContext2D;

    /**
     * 获取context2d对象
     */
    get cxt2d(): CanvasRenderingContext2D {
        return this._cxt2d;
    }

    constructor(canvas: HTMLCanvasElement) {
        this._canvas = canvas;
        this._cxt2d = <CanvasRenderingContext2D>canvas.getContext('2d');
    }

    /**
     * 清空画布
     */
    public clean() {
        this.cxt2d.clearRect(0, 0, this._canvas.width, this._canvas.height);
    }

    /**
     * 准备绘图
     * @param color 颜色
     * @param lineWidth 线宽
     */
    public readDraw(color: string, lineWidth: number) {
        this._cxt2d.save();
        this._cxt2d.beginPath();
        this._cxt2d.strokeStyle = color;
        this._cxt2d.lineWidth = lineWidth;
    }

    /**
     * 结束绘画
     */
    public drawOver() {
        this._cxt2d.stroke();
        this._cxt2d.restore();
    }

    /**
     * 颜色计算
     * @param r 红
     * @param g 绿
     * @param b 蓝
     */
    public rgb(r: number, g: number, b: number): number {
        if (r > 0 && r < 255)
            var red = Math.round(r) << 16;
        else
            red = 0;

        if (g > 0 && g < 255)
            var green = Math.round(g) << 8;
        else
            green = 0;

        if (b > 0 && b < 255)
            var blue = Math.round(b);
        else
            blue = 0;

        var color: number = red + green + blue;
        return color;
    }
} 