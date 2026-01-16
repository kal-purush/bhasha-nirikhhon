import { addCoords, Coords, equalCoords, isInside } from "../coords";
import { Patterns, zero } from "../patterns";
import { Producer } from "../producers";
import { Polygons } from "../shapes/regular-polygons";

const hexAround = (radius: number, center: Coords) =>
  Polygons.byOuterRadius(6, radius).vertices.map((c) => addCoords(c, center));

/**
 * From a given center, generates hexagons at each vertex of the previously generated hexagon,
 * until reaching boundaries.
 */
export const overlappingHexBuilder = (initialCoords: Coords = zero()) =>
  new HexBuilder(initialCoords);

class HexBuilder {
  constructor(readonly initialCoords: Coords) {}

  hexesUntil(oppositeCorner: Coords, radius: number): Patterns.Repetition {
    return new Patterns.BoundedRepetition(
      new HexesProducer(
        radius,
        this.initialCoords,
        this.initialCoords,
        oppositeCorner
      )
    );
  }
}

// recurse until all generated points are out of bounds (or already known?)
class HexesProducer implements Producer<Coords | undefined> {
  private returnQueue: Coords[] = [];
  private probeQueue: Coords[] = [];

  constructor(
    readonly radius: number,
    readonly firstCenter: Coords,
    readonly topLeft: Coords,
    readonly bottomRight: Coords
  ) {
    this.queueHex(firstCenter);
  }

  private queueHex(center: Coords): void {
    this.probeQueue.push(center);

    const newHex = hexAround(this.radius, center)
      // bypass points outside boundaries
      .filter((c) => isInside(c, this.topLeft, this.bottomRight))
      // bypass points we've already used as a "center"
      .filter((c) => !this.probeQueue.find((c2) => equalCoords(c, c2)));
    if (newHex.length > 0) {
      this.returnQueue.push(...newHex);
      console.log(`recursing into ${newHex.length} points`, newHex);
      newHex.forEach((c) => this.queueHex(c));
    }
  }

  next(): Coords | undefined {
    return this.returnQueue.shift();
  }
}