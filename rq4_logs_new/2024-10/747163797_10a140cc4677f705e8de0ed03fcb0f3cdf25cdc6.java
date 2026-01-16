package app.bpartners.geojobs.endpoint.event.model.annotation;

import static app.bpartners.geojobs.endpoint.event.model.annotation.AnnotationModelDeliveryType.FALSE_POSITIVE;

import app.bpartners.geojobs.repository.model.detection.DetectableObjectConfiguration;
import java.util.List;
import lombok.*;

@EqualsAndHashCode(callSuper = true)
@ToString
public class AnnotationFalsePositiveDeliveryRequested extends AnnotationModelDeliveryRequested {
  public AnnotationFalsePositiveDeliveryRequested(
      String zoneDetectionJobId,
      String annotationJobId,
      Double minimumConfidenceForDelivery,
      List<DetectableObjectConfiguration> detectableObjectConfigurations) {
    super(
        zoneDetectionJobId,
        annotationJobId,
        minimumConfidenceForDelivery,
        detectableObjectConfigurations);
  }

  @Override
  public AnnotationModelDeliveryType getAnnotationModelDeliveryType() {
    return FALSE_POSITIVE;
  }
}