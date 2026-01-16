import {Coordinate, Orientation, Warship} from "..";

export enum CollisionStatus {
    COLLIDE,
    MISS,
}

export class CollisionDetector {

    public static detectWarshipCollision(warshipOrigin: Warship, warshipTested: Warship): CollisionStatus {
        const originCoordinate = CollisionDetector._processCoordinate(warshipOrigin);
        const testedCoordinate = CollisionDetector._processCoordinate(warshipTested);

        console.log(originCoordinate, testedCoordinate);

        return CollisionDetector._collide(originCoordinate, testedCoordinate);
    }

    public static _collide(originCoordinates: Coordinate[], testedCoordinates: Coordinate[]): CollisionStatus {
        for (const originCoordinate of originCoordinates) {
            for (const testedCoordinate of testedCoordinates) {
               if (originCoordinate.x === testedCoordinate.x && originCoordinate.y === testedCoordinate.y) {
                   return CollisionStatus.COLLIDE;
               }
            }
        }

        return CollisionStatus.MISS;
    }

    private static _processCoordinate(warship: Warship): Coordinate[] {
        const coordinates = [];

        for (let i = 0; i < warship.size(); i++) {
            let coordinate: Coordinate;

            switch (warship.orientation()) {
                case Orientation.NORTH:
                    coordinate = new Coordinate(warship.head().x, warship.head().y + i);
                    break;
                case Orientation.SOUTH:
                    coordinate = new Coordinate(warship.head().x, warship.head().y - i);
                    break;
                case Orientation.WEST:
                    coordinate = new Coordinate(warship.head().x - i, warship.head().y);
                    break;
                case Orientation.EAST:
                    coordinate = new Coordinate(warship.head().x + i, warship.head().y);
                    break;
            }
            coordinates.push(coordinate);
        }

        return coordinates;
    }
}