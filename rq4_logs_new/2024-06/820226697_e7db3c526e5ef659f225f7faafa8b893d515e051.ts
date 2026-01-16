export class Pond {
  x: number;
  y: number;
  w: number;
  h: number;
  sprite: HTMLImageElement;

  constructor(x: number, y: number, w: number, h: number, sprite: string) {
    this.x = x;
    this.y = y;
    this.w = w;
    this.h = h;
    this.sprite = new Image();
    this.sprite.src = sprite;
  }

  /**
   * The draw function in TypeScript uses the CanvasRenderingContext2D to draw an image at a specified
   * position and size.
   * @param {CanvasRenderingContext2D} ctx - The `ctx` parameter in the `draw` function is of type
   * `CanvasRenderingContext2D`, which is a built-in HTML5 object representing a two-dimensional
   * rendering context. It provides the methods and properties to draw on the canvas.
   */
  draw(ctx: CanvasRenderingContext2D) {
    ctx.drawImage(this.sprite, 0, 0, 324, 18, this.x, this.y, this.w, this.h);
  }
}