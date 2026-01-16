import { Hit } from '../types/Hit';
import { Traceable } from '../types/Traceable';
import Normal3D from './Normal';
import Ray from './Ray';
import Vector3D from './Vector3D';
import Point3D from './Point3D';

export default class Plane implements Traceable {
  public readonly normal: Normal3D;
  public readonly point: Point3D;

  constructor(point: Point3D, normalVector: Vector3D) {
    this.normal = new Normal3D(normalVector);
    this.point = point;
  }

  public getIntersection(ray: Ray): Hit | null {
    const denominator = this.normal.vector.dotProduct(ray.vector);

    if (Math.abs(denominator) === 0) return null;

    const t =
      this.point
        .toVector()
        .subtract(ray.position.toVector())
        .dotProduct(this.normal.vector) / denominator;

    if (t < 0) return null;

    const pHit = ray.position
      .toVector()
      .add(ray.vector.multiply(t))
      .toPoint3D();

    return {
      normal: new Normal3D(
        denominator < 0 ? this.normal.vector : this.normal.vector.multiply(-1)
      ),
      point: pHit,
      t,
    };
  }
}