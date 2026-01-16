package application.service;

import application.model.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ResultsWeigher {
    public List<Row> apply(List<Row> rows) {
        return IntStream
                .rangeClosed(1, rows.size())
                .mapToObj(number -> Collections.nCopies(number, rows.get(number - 1)))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}