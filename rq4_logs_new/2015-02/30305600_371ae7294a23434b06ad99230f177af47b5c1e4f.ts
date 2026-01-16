class FontChecker {
  private _ruler: HTMLSpanElement;
  private _canvas: HTMLCanvasElement;
  private _ctx: CanvasRenderingContext2D;
  private _width: number;
  private _height: number;

  constructor(containerId: string, width: number, height: number) {
    var root = document.getElementById(containerId);
    if (!root) {
      throw new Error("Element not found for id = " + containerId);
    }

    // `span` element for text measuring
    var e = <HTMLSpanElement>document.createElement("span");
    var st = e.style;
    st.visibility = "hidden";
    st.margin = "0";
    st.padding = "0";
    st.border = "none";
    st.position = "absolute";
    st.whiteSpace = "nowrap";
    st.verticalAlign = "bottom";

    root.appendChild(e);
    this._ruler = e;

    var c = <HTMLCanvasElement>document.createElement("canvas");
    c.width = width;
    c.height = height;

    root.appendChild(c);
    this._canvas = c;
    this._ctx = this._canvas.getContext("2d");
    this._width = width;
    this._height = height;
  }

  private measureText(text: string, fontopt: any): any {
    var ruler = this._ruler;
    var st = ruler.style;
    st.fontFamily = fontopt.fontFamily;
    st.fontSize = fontopt.fontSize;
    ruler.textContent = text;
    var width = ruler.offsetWidth;
    var height = ruler.offsetHeight;
    //rulerElem.textContent = "";
    return {
      "width": width,
      "height": height
    };
  }

  private createTextCanvas(text: string, fontopt: any): HTMLCanvasElement {
    var rect = this.measureText(text, fontopt);
    var c = <HTMLCanvasElement>document.createElement("canvas");
    c.width = rect.width;
    c.height = rect.height;
    var ctx = c.getContext("2d");
    ctx.textBaseline = "bottom";
    ctx.font = fontopt.fontSize + " '" + fontopt.fontFamily + "'";
    ctx.fillText(text, 0, rect.height);

    return c;
  }

  public clear(): void {
    var ctx = this._ctx;
    ctx.clearRect(0, 0, this._width, this._height);
  }

  public draw(text: string, fontopt: any, drawopt: any): void {
    var ctx = <any>this._ctx;
    ctx.imageSmoothingEnabled = false;
    ctx.mozImageSmoothingEnabled = false;
    ctx.oImageSmoothingEnabled = false;
    ctx.webkitImageSmoothingEnabled = false;
    var textCanvas = this.createTextCanvas(text, fontopt);
    var factor = drawopt.factor;
    ctx.drawImage(textCanvas,
      0, 0, textCanvas.width, textCanvas.height,
      0, 0, factor * textCanvas.width, factor * textCanvas.height);
  }
}