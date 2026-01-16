package app.bpartners.geojobs.unit;

import static app.bpartners.geojobs.endpoint.rest.model.MultiPolygon.TypeEnum.MULTI_POLYGON;
import static app.bpartners.geojobs.endpoint.rest.model.Point.TypeEnum.POINT;
import static app.bpartners.geojobs.endpoint.rest.model.Polygon.TypeEnum.POLYGON;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.bpartners.geojobs.endpoint.rest.model.Feature;
import app.bpartners.geojobs.endpoint.rest.model.FeatureGeometry;
import app.bpartners.geojobs.endpoint.rest.model.MultiPolygon;
import app.bpartners.geojobs.endpoint.rest.model.Point;
import app.bpartners.geojobs.endpoint.rest.model.Polygon;
import app.bpartners.geojobs.endpoint.rest.validator.FeatureMultiPolygonChecker;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class FeatureMultiPolygonCheckerTest {
  private final FeatureMultiPolygonChecker subject = new FeatureMultiPolygonChecker();

  private Feature multipolygon() {
    Feature feature = new Feature();
    var coordinates =
        List.of(
            List.of(
                List.of(
                    List.of(
                        BigDecimal.valueOf(6.958009303660302),
                        BigDecimal.valueOf(43.543013820437459)),
                    List.of(
                        BigDecimal.valueOf(6.957988034043352),
                        BigDecimal.valueOf(43.54328420602328)),
                    List.of(
                        BigDecimal.valueOf(6.958082768541455),
                        BigDecimal.valueOf(43.543132354704881)),
                    List.of(
                        BigDecimal.valueOf(6.958009303660302),
                        BigDecimal.valueOf(43.543013820437459)))));
    MultiPolygon multiPolygon = new MultiPolygon().coordinates(coordinates);
    multiPolygon.setType(MULTI_POLYGON);
    feature.setGeometry(new FeatureGeometry(multiPolygon));
    feature.setId("multipolygon");
    return feature;
  }

  private Feature polygon() {
    Feature feature = new Feature();
    var coordinates =
        List.of(
            List.of(
                List.of(
                    BigDecimal.valueOf(6.958009303660302), BigDecimal.valueOf(43.543013820437459)),
                List.of(
                    BigDecimal.valueOf(6.957965493371299), BigDecimal.valueOf(43.543002082885863)),
                List.of(
                    BigDecimal.valueOf(6.957822106008073), BigDecimal.valueOf(43.543033084979541)),
                List.of(
                    BigDecimal.valueOf(6.957796040201745),
                    BigDecimal.valueOf(43.543066366941567))));
    var polygon = new Polygon().coordinates(coordinates);
    polygon.setType(POLYGON);
    feature.setGeometry(new FeatureGeometry(polygon));
    feature.setId("polygon");
    return feature;
  }

  private Feature point() {
    Feature feature = new Feature();
    var coordinates =
        List.of(BigDecimal.valueOf(6.958009303660302), BigDecimal.valueOf(43.543013820437459));
    var point = new Point().coordinates(coordinates);
    point.setType(POINT);
    feature.setGeometry(new FeatureGeometry(point));
    feature.setId("point");
    return feature;
  }

  @Test
  void check_list_of_features() {
    assertTrue(subject.apply(List.of(multipolygon())));
    assertFalse(subject.apply(List.of(polygon())));
    assertFalse(subject.apply(List.of(point())));
  }
}