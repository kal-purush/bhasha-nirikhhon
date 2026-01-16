package edu.agh.utils;

import javafx.util.StringConverter;
import org.apache.commons.math3.analysis.UnivariateFunction;

import java.util.Arrays;
import java.util.List;

public class FunctionConverter extends StringConverter<Pair<String, UnivariateFunction>> {

    public static List<Pair<String, UnivariateFunction>> getStringUnivariateFunctionPairs() {
        return Arrays.asList(
                new Pair<>("y = 1/x", Functions.function1()),
                new Pair<>("y = x", Functions.function2()),
                new Pair<>("y = -x + 100", Functions.function3()),
                new Pair<>("<0;50> y = x | (50;100> y = -x + 100", Functions.function4())
        );
    }

    @Override
    public String toString(Pair<String, UnivariateFunction> pair) {
        return pair.getLeftValue();
    }

    @Override
    public Pair<String, UnivariateFunction> fromString(String string) {
        List<Pair<String, UnivariateFunction>> pairs = getStringUnivariateFunctionPairs();

        return pairs.stream()
                .filter(p -> p.getLeftValue().equals(string))
                .findFirst()
                .get();
    }

}