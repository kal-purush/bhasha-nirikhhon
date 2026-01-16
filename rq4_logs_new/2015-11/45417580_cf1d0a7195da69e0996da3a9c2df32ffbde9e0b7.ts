class Point {
  x: number;
  y: number;

  constructor({x, y}) {
    this.x = x;
    this.y = y;
  }

  add(other: Point) {
    this.x += other.x;
    this.y += other.y;
    return this;
  }

  sub(other: Point) {
    this.x -= other.x;
    this.y -= other.y;
    return this;
  }

  mul(num: number) {
    this.x *= num;
    this.y *= num;
    return this;
  }

  clone() {
    return new Point(this);
  }
}

/**
 * Returns a path, between every two control points numPoints are generated and the
 * control points themselves are added too. The first and the last controlpoint are
 * duplicated to give correct values at the ends of the path.
 * If there's less than 2 controlpoints an empty path is returned.
 *
 * For more information on this type of spline see http://www.mvps.org/directx/articles/catmull/.
 */
export function catmullRomSpline(points: {x: number, y: number}[], numPoints: number) {
  const newPoints = [];
  const controlPoints = points.map(point => new Point(point));
  controlPoints.splice(0, 0, controlPoints[0]);
  controlPoints.push(controlPoints[controlPoints.length - 1]);

  if (controlPoints.length < 4) {
    return controlPoints;
  }

  for (let i = 1; i <= controlPoints.length - 3; i++) {
    newPoints.push(controlPoints[i]);
    const increment = 1 / (numPoints + 1);
    let t = increment;

    const t1 = controlPoints[i + 1].clone().sub(controlPoints[i - 1]).mul(0.5);
    const t2 = controlPoints[i + 2].clone().sub(controlPoints[i]).mul(0.5);

    for (let j = 0; j < numPoints; j++) {
      const h1 = 2 * t * t * t - 3 * t * t + 1;
      const h2 = -2 * t * t * t + 3 * t * t;
      const h3 = t * t * t - 2 * t * t + t;
      const h4 = t * t * t - t * t;

      const point = new Point(controlPoints[i]).mul(h1);
      point.add(controlPoints[i + 1].clone().mul(h2));
      point.add(t1.clone().mul(h3));
      point.add(t2.clone().mul(h4));
      newPoints.push(point);
      t += increment;
    }
  }

  if (controlPoints.length >= 4) {
      newPoints.push(controlPoints[controlPoints.length - 2]);
  }

  return newPoints;
}