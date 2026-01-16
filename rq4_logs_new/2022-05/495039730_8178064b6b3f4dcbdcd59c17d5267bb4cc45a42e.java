package calculator;

import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExpressionParser {

    private final String PATTERN = "^\\d+( [\\+|\\-|\\*|/|] \\d+)+$";

    public boolean valid(String expr) {
        return Pattern.matches(PATTERN, expr);
    }

    public Iterator<String> parseExpr(String expr) {
        return Stream.of(expr.split(" ")).collect(Collectors.toList()).iterator();
    }

}