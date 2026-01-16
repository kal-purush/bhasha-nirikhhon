package upeldev.com.github.upel3.model;

import lombok.Getter;

import java.util.List;

@Getter
public enum ElementAggregation {
    SUM,
    AVG,
    WAVG;

    public static<T extends Aggregable> double countSum(List<T> subElements){
        return subElements.stream().mapToDouble(Aggregable::getAggregableValue).sum();
    }

    public static<T extends Aggregable> double countAvg(List<T> subElements){
        return subElements.stream().mapToDouble(Aggregable::getAggregableValue).average().orElse(0);
    }

    public static<T extends Aggregable> double countWavg(List<T> subElements){
        double numerator = subElements.stream().mapToDouble(subElement -> subElement.getAggregableValue() * subElement.getAggregableWeight()).sum();
        double denominator = subElements.stream().mapToDouble(subElement -> subElement.getAggregableWeight()).sum();
        return numerator / denominator;
    }

//    public static double countMaxSum(List<SubActivity> subActivities){
//        return subActivities.stream().mapToDouble(SubActivity::getMaxValue).sum();
//    }
//
//    public static double countMaxAvg(List<SubActivity> subActivities){
//        return subActivities.stream().mapToDouble(SubActivity::getMaxValue).average().orElse(0);
//    }
//
//    public static double countMaxWavg(List<SubActivity> subActivities){
//        double numerator = subActivities.stream().mapToDouble(subActivity -> subActivity.getMaxValue() * subActivity.getWeight()).sum();
//        double denominator = subActivities.stream().mapToDouble(SubActivity::getWeight).sum();
//        return numerator / denominator;
//    }

    @Override
    public String toString() {
        switch (this) {
            case SUM: return "Suma";
            case AVG: return"Średnia";
            case WAVG: return "Średnia ważona";
        }
        return "";
    }
}