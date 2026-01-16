package com.github.fakemongo.impl.geo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.vividsolutions.jts.geom.Coordinate;
import org.geojson.*;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.util.Arrays.array;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class GeoUtilTest {

  @Test
  public void testGeoJsonPointsCanBeConvertedToJTSGeometry() throws Exception {
    final Point geoJson = new Point(pos(-1, -1));
    final DBObject dbObject = toDBObject(geoJson);

    final com.vividsolutions.jts.geom.Geometry geometry = GeoUtil.toGeometry(dbObject);

    assertThat(geometry, instanceOf(com.vividsolutions.jts.geom.Point.class));
    assertArrayEquals(array(coord(-1, -1)), geometry.getCoordinates());
  }

  @Test
  public void testGeoJsonMultiPointsCanBeConvertedToJTSGeometry() throws Exception {
    final MultiPoint geoJson = new MultiPoint(pos(-1, -1), pos(1, 1));
    final DBObject dbObject = toDBObject(geoJson);

    final com.vividsolutions.jts.geom.Geometry geometry = GeoUtil.toGeometry(dbObject);

    assertThat(geometry, instanceOf(com.vividsolutions.jts.geom.MultiPoint.class));
    assertArrayEquals(array(coord(-1, -1), coord(1, 1)), geometry.getCoordinates());
  }

  @Test
  public void testGeoJsonPolygonsCanBeConvertedToJTSGeometry() throws Exception {
    final Polygon geoJson = new Polygon(createRing(pos(-1, -1), pos(1, -1), pos(0, 1), pos(-1, -1)));
    final DBObject dbObject = toDBObject(geoJson);

    final com.vividsolutions.jts.geom.Geometry geometry = GeoUtil.toGeometry(dbObject);

    assertThat(geometry, instanceOf(com.vividsolutions.jts.geom.Polygon.class));
    assertArrayEquals(array(coord(-1, -1), coord(-1, 1), coord(1, 0), coord(-1, -1)), geometry.getCoordinates());
  }

  @Test
  public void testGeoJsonMultiPolygonsCanBeConvertedToJTSGeometry() throws Exception {
    final MultiPolygon geoJson = new MultiPolygon(new Polygon(createRing(pos(-1, -1), pos(1, -1), pos(0, 1), pos(-1, -1))));
    final DBObject dbObject = toDBObject(geoJson);

    final com.vividsolutions.jts.geom.Geometry geometry = GeoUtil.toGeometry(dbObject);

    assertThat(geometry, instanceOf(com.vividsolutions.jts.geom.MultiPolygon.class));
    assertArrayEquals(array(coord(-1, -1), coord(-1, 1), coord(1, 0), coord(-1, -1)), geometry.getCoordinates());
  }

  private Coordinate coord(int x, int y) {
    return new Coordinate(x, y);
  }

  private LngLatAlt pos(int longitude, int latitude) {
    return new LngLatAlt(longitude, latitude);
  }

  private List<LngLatAlt> createRing(LngLatAlt... coordinates) {
    return asList(coordinates);
  }

  private DBObject toDBObject(GeoJsonObject geoJsonGeometry) throws JsonProcessingException {
    return (DBObject) JSON.parse(new ObjectMapper().writeValueAsString(geoJsonGeometry));
  }
}