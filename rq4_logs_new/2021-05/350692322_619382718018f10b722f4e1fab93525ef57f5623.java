package upeldev.com.github.upel3.model;

import lombok.Getter;

import java.util.List;

@Getter
public enum GradeAggregation {
    SUM,
    AVG,
    WAVG;

    public static double countSum(List<SubGrade> subGrades){
        return subGrades.stream().mapToDouble(SubGrade::getValue).sum();
    }

    public static double countAvg(List<SubGrade> subGrades){
        return subGrades.stream().mapToDouble(SubGrade::getValue).average().orElse(0);
    }

    public static double countWavg(List<SubGrade> subGrades){
        double numerator = subGrades.stream().mapToDouble(subGrade -> subGrade.getValue() * subGrade.getWeight()).sum();
        double denominator = subGrades.stream().mapToDouble(SubGrade::getWeight).sum();
        return numerator / denominator;
    }
}