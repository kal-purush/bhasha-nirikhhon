package homework.day13;

import java.util.stream.Stream;

public class ElementsRunner {
    public static void main(String[] args) {

        Stream<String> elements = Stream.of("Text field", "Radio", "Check-box", "Drop-down", "Picker",
                "Breadcrumb");

        elements.map(x -> {
                    if (x.length() % 2 != 0) {
                        return x.replace("e", "o");
                    } else {
                        return String.valueOf(x.length());
                    }
                }
        ).distinct().forEach(System.out::println);
    }
}
