import Point3D from '../src/objects/Point3D';
import Vector3D from '../src/objects/Vector3D';
import Ray from '../src/objects/Ray';

describe('Point3D', () => {
  const pointA = new Point3D(0, 0, 0);
  const pointB = new Point3D(1, 2, 3);

  describe('getLengthTo', () => {
    it('returns the correct length between two vertices', () => {
      const length = pointA.getLengthTo(pointB);
      expect(length).toBe(Math.sqrt(14));
    });
  });

  describe('toVector', () => {
    it('returns a Vector3D object with the same values as the vertex', () => {
      const vector = pointA.toVector();
      expect(vector.x).toBe(0);
      expect(vector.y).toBe(0);
      expect(vector.z).toBe(0);
    });
  });

  describe('isInsideRay', () => {
    const ray = new Ray(new Point3D(0, 0, 0), new Vector3D(1, 0, 0));

    it('returns true for a vertex that is on the ray', () => {
      const pointC = new Point3D(3, 0, 0);
      expect(pointC.isInsideRay(ray)).toBe(true);
    });

    it('returns true for a vertex that is in front of the ray', () => {
      const pointD = new Point3D(3, 1, 1);
      expect(pointD.isInsideRay(ray)).toBe(true);
    });

    it('returns false for a vertex that is behind the ray', () => {
      const pointE = new Point3D(-1, 1, 1);
      expect(pointE.isInsideRay(ray)).toBe(false);
    });
  });
});