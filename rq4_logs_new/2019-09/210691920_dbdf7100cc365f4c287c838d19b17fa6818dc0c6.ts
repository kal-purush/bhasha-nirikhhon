import {Carrier, Coordinate, Destroyer, Frigate, Orientation} from "../src";
import {CollisionDetector, CollisionStatus} from "../src/utils/CollisionDetector";

describe("Collision Detector - Behavior", () => {
    it("should detect collision between two warship", () => {
        const titanic = new Destroyer();
        titanic.setPosition(new Coordinate(0, 0), Orientation.NORTH);
        const iceberg = new Carrier();
        iceberg.setPosition(new Coordinate(3, 1), Orientation.WEST);

        expect(CollisionDetector.detectWarshipCollision(iceberg, titanic)).toBe(CollisionStatus.COLLIDE);
    });

    it("should not detect collision between two distant warship", () => {
        const sun = new Carrier();
        sun.setPosition(new Coordinate(0, 0), Orientation.EAST);
        const earth = new Frigate();
        earth.setPosition(new Coordinate(1, 1), Orientation.SOUTH);

        expect(CollisionDetector.detectWarshipCollision(sun, earth)).toBe(CollisionStatus.MISS);
    });
});