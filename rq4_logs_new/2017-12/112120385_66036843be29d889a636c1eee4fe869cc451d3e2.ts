
type Coordinate = [number, number];

export class CanvasUtility {

  private static readonly DASH_ARRAY = [8, 4];
  private context: CanvasRenderingContext2D;

  constructor(private canvas: HTMLCanvasElement, private readonly width: number,
              private readonly height: number) {
    const context = canvas.getContext("2d");
    if (context === null) {
      throw new Error("Unable to obtain 2D context for canvas.");
    } else {
      this.context = context;
    }
  }

  /**
   * Dispatches a canvas click event.  Does a little quick math to determine where on the canvas the click
   * happened.
   *
   * @param event Mouse event from the HTML
   */
  public getEventPosition(event: MouseEvent): Coordinate {
    const boundingRect = this.canvas.getBoundingClientRect();

    const xValue = event.clientX - boundingRect.left;
    const yValue = event.clientY - boundingRect.top;

    return [xValue, yValue];
  }

  /**
   * Clears the contents of the canvas.  Does not account for transforms to the canvas.
   * @param zoom Zoom level to clear the canvas for
   */
  public clearCanvas(zoom: number) {
    this.context.clearRect(0, 0, this.width / zoom, this.height / zoom);
  }

  /**
   * Draws the outlines of the pixels on the canvas.  Draws a dashed line, but does reset that back
   * to its old value.
   * @param zoom Zoom level to draw the outlines for.  Only affects the size of the line.
   * @param pixelWidth Width of each "pixel" on the canvas
   * @param pixelHeight Height of each "pixel" on the canvas
   */
  public drawPixelOutlines(zoom: number, pixelWidth: number, pixelHeight: number) {
    const oldLineDash = this.context.getLineDash();
    this.context.setLineDash(CanvasUtility.DASH_ARRAY);
    this.context.lineWidth = 1 / zoom;

    // TODO: Don't dupe the code
    // Draw the horizontal lines
    const barWidth = this.width / pixelWidth;
    let linePosition = barWidth;
    for (let i = 1; i < pixelWidth; i++) {
      this.drawLine(this.context, [linePosition, 0], [linePosition, this.height]);

      linePosition += barWidth;
    }

    // Draw the vertical lines
    const barHeight = this.height / pixelHeight;
    linePosition = barHeight;
    for (let i = 1; i < pixelHeight; i++) {
      this.drawLine(this.context, [0, linePosition], [this.width, linePosition]);

      linePosition += barHeight;
    }
    this.context.setLineDash(oldLineDash);
  }

  /**
   * Draws the pixels from the canvas data into the context
   * @param pixels Pixels to draw on the canvas
   * @param pixelWidth Width of each "pixel" on the canvas
   * @param pixelHeight Height of each "pixel" on the canvas
   */
  public drawPixels(pixels: string[][], pixelWidth: number, pixelHeight: number) {
    const drawWidth = this.width / pixelWidth;
    const drawHeight = this.height / pixelHeight;
    pixels.forEach((column, xIndex) => {
      column.forEach((pixel, yIndex) => {
        this.context.fillStyle = pixel;
        this.context.fillRect(xIndex * drawWidth, yIndex * drawHeight, drawWidth, drawHeight);
      });
    });
  }

  public setZoom(zoomValue: number) {
    this.context.setTransform(zoomValue, 0, 0, zoomValue, 0, 0);
  }

  /**
   * Draws a single line from the given start coordinate to the end.
   * @param context Canvas context to draw the line on
   * @param start Start coordinate to use
   * @param end End coordinate to use
   */
  private drawLine(context: CanvasRenderingContext2D, start: Coordinate, end: Coordinate) {
    context.beginPath();
    context.moveTo(start[0], start[1]);
    context.lineTo(end[0], end[1]);
    context.stroke();
  }
}