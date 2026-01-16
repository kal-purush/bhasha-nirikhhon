import java.util.HashMap;
import java.util.Map;

public class CalculationFactory {

    private static CalculationFactory operation = new CalculationFactory();

    private CalculationFactory() {
    }

    public static CalculationFactory getInstance() {
        return operation;
    }

    public Calculation getCalculation(String operator) {

        Map<String, Calculation> map = new HashMap<>();
        map.put("+", MathOperation.ADDITION);
        map.put("-", MathOperation.SUBTRACTION);
        map.put("*", MathOperation.MULTIPLICATION);
        map.put("/", MathOperation.DIVISION);

        return map.get(operator);
    }
}

