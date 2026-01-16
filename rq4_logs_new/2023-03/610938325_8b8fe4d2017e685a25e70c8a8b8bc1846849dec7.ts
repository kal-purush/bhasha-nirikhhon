import Point3D from '../src/objects/Point3D';
import Vector3D from '../src/objects/Vector3D';
import Ray from '../src/objects/Ray';

describe('Ray', () => {
  const position = new Point3D(0, 0, 0);
  const vector = new Vector3D(1, 0, 0);
  const ray = new Ray(position, vector);

  describe('hasInside', () => {
    it('returns true for a point that is on the ray', () => {
      const pointA = new Point3D(3, 0, 0);
      expect(ray.hasInside(pointA)).toBe(true);
    });

    it('returns true for a point that is in front of the ray', () => {
      const pointB = new Point3D(3, 1, 1);
      expect(ray.hasInside(pointB)).toBe(true);
    });

    it('returns false for a point that is behind the ray', () => {
      const pointC = new Point3D(-1, 1, 1);
      expect(ray.hasInside(pointC)).toBe(false);
    });
  });

  describe('angleBetweenRads', () => {
    it('should return a correct angle between another vector', () => {
      const v1 = new Vector3D(1, 0, 0);
      const v2 = new Vector3D(1, 1, 0);
      expect(
        new Ray(new Point3D(0, 3, 1), v1).angleBetweenRads(
          new Ray(new Point3D(13, 3, 2), v2)
        )
      ).toBeCloseTo(Math.PI / 4);
    });
  });
});