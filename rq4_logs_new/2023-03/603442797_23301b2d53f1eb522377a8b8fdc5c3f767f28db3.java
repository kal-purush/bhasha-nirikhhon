package homework.day13;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ButterfliesRunner {
    public static void main(String[] args) {
        List<String> butterflies = new ArrayList<>();
        butterflies.add("Common blue");
        butterflies.add("Swallowtail");
        butterflies.add("Aglais io");
        butterflies.add("Common blue");

        butterflies.stream().map(x -> "\"" + x + "\"").flatMap(x -> Arrays.stream(x.split(" "))).filter(x -> x.contains("a") && x.contains("o"))
                .forEach(System.out::println);

    }

}