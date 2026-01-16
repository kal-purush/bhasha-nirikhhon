import Vec2D from "@/math/vec2D";
import Vec4D from "@/math/vec4D";
type DragPoint = "a" | "b";
export class Structure {
  //TODO: should i check is there a structure with given dimensions already?
  private objectVec: Vec4D;
  private colliderVec: Vec4D;
  private anchorVec: Vec2D;
  private id: string;
  public dragPoint: DragPoint;
  public includedTiles: Set<number>;
  public colliderTiles: Set<number>;
  public anchorTile: number | undefined;
  public static tileSize: Size2D = { w: 32, h: 32 };
  constructor(pointA: Position2D, pointB: Position2D) {
    this.id = crypto.randomUUID();
    this.objectVec = Vec4D.create([
      Math.min(pointA.x, pointB.x),
      Math.min(pointA.y, pointB.y),
      Math.max(pointA.x, pointB.x) + Structure.tileSize.w,
      Math.max(pointA.y, pointB.y) + Structure.tileSize.h,
    ]);
    this.colliderVec = Vec4D.create([0, 0, 0, 0]);
    this.anchorVec = Vec2D.create([0, 0]);
    this.dragPoint = "b";
    this.includedTiles = new Set();
    this.colliderTiles = new Set();
    this.anchorTile = undefined;
  }
  public get pointsPath() {
    return {
      A: { x: this.objectVec.x, y: this.objectVec.y },
      B: { x: this.objectVec.z, y: this.objectVec.w },
    };
  }
  public get vecPath() {
    return {
      A: Vec2D.create([this.objectVec.x, this.objectVec.y]),
      B: Vec2D.create([this.objectVec.z, this.objectVec.w]),
    };
  }
  public get pointsCollider() {
    return {
      A: { x: this.colliderVec.x, y: this.colliderVec.y },
      B: { x: this.colliderVec.z, y: this.colliderVec.w },
    };
  }
  public get vecCollider() {
    return {
      A: Vec2D.create([this.colliderVec.x, this.colliderVec.y]),
      B: Vec2D.create([this.colliderVec.z, this.colliderVec.w]),
    };
  }
  public get pointsAnchor() {
    return { x: this.anchorVec.x, y: this.anchorVec.y };
  }
  public get vecAnchor() {
    return Vec2D.create([this.anchorVec.x, this.anchorVec.y]);
  }

  public get getID() {
    return this.id;
  }
  public deleteCollider() {
    this.colliderVec = Vec4D.create([0, 0, 0, 0]);
    this.colliderTiles.clear();
  }
  public deleteAnchor() {
    this.anchorVec = Vec2D.create([0, 0]);
    this.anchorTile = undefined;
  }
  public setCollider(pointA: Position2D, pointB: Position2D) {
    this.colliderVec = Vec4D.create([
      pointA.x,
      pointA.y,
      pointB.x + Structure.tileSize.w,
      pointB.y + Structure.tileSize.h,
    ]);
  }
  public setAnchor(anchor: Position2D) {
    this.anchorVec = Vec2D.create([anchor.x, anchor.y]);
  }

  public recalculateObject(val: Position2D) {
    if (
      this.dragPoint === "a" &&
      val.x < this.pointsPath.B.x &&
      val.y < this.pointsPath.B.y
    )
      this.objectVec = Vec4D.create([
        val.x,
        val.y,
        this.objectVec.z,
        this.objectVec.w,
      ]);
    else if (
      this.dragPoint === "b" &&
      val.x + 1 > this.pointsPath.A.x &&
      val.y + 1 > this.pointsPath.A.y
    )
      this.objectVec = Vec4D.create([
        this.objectVec.x,
        this.objectVec.y,
        val.x + Structure.tileSize.w,
        val.y + Structure.tileSize.h,
      ]);
  }

  public isDragged(mouse: Position2D, dist: number) {
    const mouseVec = Vec2D.create([mouse.x, mouse.y]);
    const pointA = this.vecPath.A.distanceToOtherVec2D(mouseVec);
    const pointB = this.vecPath.B.distanceToOtherVec2D(mouseVec);
    if (pointA < dist) {
      this.dragPoint = "a";
      return true;
    } else if (pointB < dist) {
      this.dragPoint = "b";
      return true;
    }
    return false;
  }
}