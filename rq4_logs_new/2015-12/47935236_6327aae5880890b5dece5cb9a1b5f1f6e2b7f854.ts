class ParallaxLayer extends Sprite {
  private _texture: PIXI.Texture;
  private _tiles: MagicDict<Point, Sprite>;
  private _parallax: number;

  constructor(texture: PIXI.Texture | string, parallax: number = .2) {
    super();

    this._parallax = parallax;
    this._tiles = new MagicDict<Point, Sprite>();

    if (texture instanceof PIXI.Texture) {
      this._texture = texture;
    } else if (typeof texture === "string") {
      this._texture = PIXI.Texture.fromImage(texture);
    }

    Globals.camera.addParallaxLayer(this, parallax);

    this.z = -100;
  }

  update(): void {
    const width = 256;
    const height = 256;

    const topLeft     = Globals.camera.topLeft(this._parallax);
    const bottomRight = Globals.camera.bottomRight(this._parallax);

    const startX = Math.floor(topLeft.x / width) * width;
    const startY = Math.floor(topLeft.y / height) * height;

    const endX   = Math.ceil(bottomRight.x / width) * width;
    const endY   = Math.ceil(bottomRight.y / height) * height;

    const positions: Point[] = [];

    for (let x = startX; x < endX; x += width) {
      for (let y = startY; y < endY; y += height) {
        const point = new Point(x, y);

        positions.push(point);

        if (!this._tiles.contains(point)) {
          const tile = new Sprite(this._texture);

          tile.x = point.x;
          tile.y = point.y;

          this.addChild(tile);
          this._tiles.put(point, tile);
        }
      }
    }
  }
}