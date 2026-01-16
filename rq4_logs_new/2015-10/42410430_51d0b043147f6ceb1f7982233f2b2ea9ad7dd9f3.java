import java.io.Serializable;
import java.util.List;

/**
 * Created by Jianhong Li on 9/28/15.
 */
public class DataRow implements Serializable {
  DataRow(List<Double> features, int label) {
    this.features = features;
    this.label = label;
  }

  public List<Double> features;
  public int label;
}