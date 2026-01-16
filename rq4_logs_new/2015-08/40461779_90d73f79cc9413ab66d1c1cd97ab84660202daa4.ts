class Cell {
  private static COLOR_LIVE = '#00aa00';
  private static COLOR_DEAD = '#000000';

  private _rect:fabric.IRect;
  private _live:boolean;
  private _canvas:fabric.ICanvas;

  constructor(canvas:fabric.ICanvas, rect:fabric.IRect, live:boolean) {
    this._canvas = canvas;
    this._rect = rect;
    this._live = live;

    this._rect.selectable = false;
    this._canvas.add(rect);
    this.updateColor();

    this._canvas.on('mouse:down', (e) => {
      if (e.target === this._rect) {
        this.live = !this.live;
      }
    });
  }

  get live():boolean {
    return this._live;
  }

  set live(value:boolean) {
    this._live = value;
    this.updateColor();
  }

  private updateColor() {
    this._rect.setFill(this._live ? Cell.COLOR_LIVE : Cell.COLOR_DEAD);
  }
}