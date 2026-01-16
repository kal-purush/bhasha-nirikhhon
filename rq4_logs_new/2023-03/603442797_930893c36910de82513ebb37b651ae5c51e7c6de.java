package homework.day11;

import java.util.ArrayList;
import java.util.List;

public class MyButterflies {
    public static void main(String[] args) {
        List<String> butterflies = new ArrayList<>();
        butterflies.add("Common blue");
        butterflies.add("Swallowtail");
        butterflies.add("Aglais io");
        butterflies.add("Common blue");

        for (String butterfly : butterflies) {
            System.out.print("\"" + butterfly + "\"" + " ");
        }
        System.out.println();

        int counter = 0;

        for (String butterfly : butterflies) {
            if (butterfly.contains("o")) {
                counter++;
            }
        }
        System.out.println(counter);

        for (int i = 0; i < butterflies.size(); i++) {
            System.out.print(butterflies.get(i) + " ");
        }
        System.out.println();

        for (String butterfly : butterflies) {
            System.out.println(butterfly);
        }
    }
}