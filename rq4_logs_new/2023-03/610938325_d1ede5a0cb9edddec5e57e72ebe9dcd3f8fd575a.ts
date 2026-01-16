import Point from "./Point";
import Vector from "./Vector";

class Ray {
    public origin : Point;
    public direction: Vector;

    constructor(origin: Point, direction : Vector) {
        this.origin = origin;
        this.direction = direction.Normalize();
    }

    public PointAt(t: number): Point {
        const x = this.origin.x + t * this.direction.x;
        const y = this.origin.y + t * this.direction.y;
        const z = this.origin.z + t * this.direction.z;

        return new Point(x,y,z);
    }
}
export default Ray