package app.bpartners.geojobs.service.detection;

import app.bpartners.geojobs.repository.model.detection.DetectableObjectModelStringMapValue;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

@Component
public class DetectableObjectModelMapper
    implements Function<Object, List<DetectableObjectModelStringMapValue>> {

  @SneakyThrows
  @Override
  public List<DetectableObjectModelStringMapValue> apply(Object modelInstance) {
    var detectableObjectStringValues = new ArrayList<DetectableObjectModelStringMapValue>();
    Class clazz = modelInstance.getClass();
    List<Field> fields = List.of(clazz.getDeclaredFields());
    var modelName = fields.get(1);
    modelName.setAccessible(true);
    var instance = fields.get(2);
    instance.setAccessible(true);
    detectableObjectStringValues.add(
        new DetectableObjectModelStringMapValue(
            modelName.get(modelInstance).toString(), instance.get(modelInstance).toString()));
    for (int i = 3; i < fields.size() - 1; i += 2) {
      Field fieldName = fields.get(i);
      Field fieldValue = fields.get(i + 1);
      fieldName.setAccessible(true);
      fieldValue.setAccessible(true);
      var name = fieldName.get(modelInstance).toString();
      var value = fieldValue.get(modelInstance).toString().equals("true") ? "oui" : "non";
      detectableObjectStringValues.add(new DetectableObjectModelStringMapValue(name, value));
    }
    return detectableObjectStringValues;
  }
}