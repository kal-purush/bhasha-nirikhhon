export const getSpatialBoundsIntersect = (bounds1, bounds2) => {
  const intersectSpatialBounds = {
    north: bounds1.north < bounds2.north ? bounds1.north : bounds2.north,
    east: bounds1.east < bounds2.east ? bounds1.east : bounds2.east,
    south: bounds1.south > bounds2.south ? bounds1.south : bounds2.south,
    west: bounds1.west > bounds2.west ? bounds1.west : bounds2.west,
  }
  if (
      intersectSpatialBounds.north < intersectSpatialBounds.south ||
      intersectSpatialBounds.east < intersectSpatialBounds.west
      ) {
      return null;
  } else {
    return intersectSpatialBounds;
  }
}

export const gridPolygonToSpatialBounds = (polygon) => {
  const coordinates = polygon.geometry.coordinates[0];
  const eastWests = coordinates.map(e=>e[0]);
  const northSouths = coordinates.map(e=>e[1]);

  return {
    north: Math.max(...northSouths),
    east: Math.max(...eastWests),
    south: Math.min(...northSouths),
    west: Math.min(...eastWests),
  }

}