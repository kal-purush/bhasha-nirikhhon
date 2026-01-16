///<reference path="DefinitelyTyped/d3/d3.d.ts" />

class Dimension {
  protected aWidth: d3.Primitive;
  protected aHeight: d3.Primitive;

  public constructor(width: d3.Primitive, height: d3.Primitive) {
    this.aHeight = height;
    this.aWidth = width;
  }

  /**
   * Getter / Setter of `width`
   * @param w
   * @returns {d3.Primitive}
   */
  public width(w: d3.Primitive): d3.Primitive {
    if (w) {
      if (+w > 0) {
        this.aWidth = w;
      }
      else {
        throw new Error("Width msut be a number and greater than 0. ");
      }
    }
    return this.aWidth;
  }

  /**
   * Getter / Setter of `height`
   * @param h
   * @returns {d3.Primitive}
   */
  public height(h: d3.Primitive): d3.Primitive {
    if (h) {
      if (+h > 0) {
        this.aHeight = h;
      }
      else {
        throw new Error("Height must be a number and greater than 0. ");
      }
    }
    return this.aHeight;
  }

}