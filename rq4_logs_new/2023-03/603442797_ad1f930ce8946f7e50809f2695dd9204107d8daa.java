package homework.day13;

import java.util.stream.Stream;

public class CountriesRunner {
    public static void main(String[] args) {
        Stream<String> countries = Stream.of("Андора", "Португалия", "Англия", "Замбия");

        countries.filter(x -> x.contains("а") || x.contains("е") || x.contains("ё") || x.contains("и") ||
                        x.contains("о") || x.contains("у") || x.contains("ы") || x.contains("э")
                        || x.contains("я") || x.contains("А") || x.contains("Е") || x.contains("Ё") ||
                        x.contains("О") || x.contains("У") || x.contains("Ы") || x.contains("Э") || x.contains("Ю")
                        || x.contains("Я")).filter(x -> x.length() < 7).map(String::toUpperCase).
                map(x -> "\"" + x + "\"").forEach(System.out::println);

    }
}