package emotionmining.model;

/**
 * Created by dhruv on 06/06/17.
 */
public class FeatureTuple {
    private int index;
    private double featureValue;

    public FeatureTuple(int index, double featureValue){
        this.index = index;
        this.featureValue = featureValue;
    }


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getFeatureValue() {
        return featureValue;
    }

    public void setFeatureValue(double featureValue) {
        this.featureValue = featureValue;
    }
}