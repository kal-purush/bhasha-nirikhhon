import java.util.Arrays;

public enum MathOperation {
    PLUS("+"), MINUS("-"), MULTIPLY("*"), DIVIDE("/");
    private String operator;
    MathOperation(String operator) {}

    public static MathOperation of(String operator) {
        return Arrays.stream(MathOperation.values())
                .filter(it -> it.operator.equals(operator))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("지원하지 않는 연산기호 입니다."));
    }

    public static boolean isNotMathOperation(String operator) {
        return Arrays.stream(MathOperation.values())
                .noneMatch(it -> it.operator.equals(operator));

    }
}