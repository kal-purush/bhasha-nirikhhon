import Ray from './Ray';
import Vector3D from './Vector3D';

export default class Point3D {
  public readonly x: number;
  public readonly y: number;
  public readonly z: number;

  constructor(x: number, y: number, z: number) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  public getLengthTo(point: Point3D) {
    return Math.sqrt(
      (this.x - point.x) * (this.x - point.x) +
        (this.y - point.y) * (this.y - point.y) +
        (this.z - point.z) * (this.z - point.z)
    );
  }

  public toVector() {
    return new Vector3D(this.x, this.y, this.z);
  }

  public isInsideRay(ray: Ray) {
    return ray.hasInside(this);
  }
}