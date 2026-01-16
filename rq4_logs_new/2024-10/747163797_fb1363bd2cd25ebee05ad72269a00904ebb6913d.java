package app.bpartners.geojobs.endpoint.rest.validator;

import app.bpartners.geojobs.endpoint.rest.model.Feature;
import app.bpartners.geojobs.endpoint.rest.model.MultiPolygon;
import java.util.List;
import java.util.function.Function;
import org.springframework.stereotype.Component;

@Component
public class FeatureMultiPolygonChecker implements Function<List<Feature>, Boolean> {
  @Override
  public Boolean apply(List<Feature> features) {
    return features.stream()
        .allMatch(
            feature -> {
              var geometry = feature.getGeometry();
              return geometry != null && geometry.getActualInstance().equals(MultiPolygon.class);
            });
  }
}