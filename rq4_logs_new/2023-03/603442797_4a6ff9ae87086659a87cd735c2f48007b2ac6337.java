package classwork.day12.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Count {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>(Arrays.asList("мама", "мыла", "раму", "мама", "чисто"));
        int i = 0;
        for (String listm : list) {
            if (listm.equals("мама")) {
                i++;
            }
        }
        System.out.println(i);

        System.out.println(list.stream().findFirst().orElse("мама"));
        System.out.println(list.stream().filter("мама"::equals).findFirst().get());
        System.out.println(list.stream().skip(4).findFirst().get());
        System.out.println(list.stream().skip(2).limit(2).collect(Collectors.toList()));
        System.out.println(list.stream().filter(x -> x.contains("м")).distinct().collect(Collectors.toList()));

    }
}
