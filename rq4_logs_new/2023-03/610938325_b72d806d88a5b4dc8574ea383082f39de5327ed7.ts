import Vector from "./Vector"

class Point {
    x: number
    y: number
    z: number

    constructor(x: number, y: number, z: number) {
        this.x = x
        this.y = y
        this.z = z
    }

    Sum(point: Point, vector: Vector): Point {
        return new Point(
            point.x + vector.x,
            point.y + vector.y,
            point.z + vector.z
        )
    }

    Difference(point: Point, vector: Vector): Point {
        return new Point(
            point.x + vector.x,
            point.y + vector.y,
            point.z + vector.z
        )
    }
}

export default Point