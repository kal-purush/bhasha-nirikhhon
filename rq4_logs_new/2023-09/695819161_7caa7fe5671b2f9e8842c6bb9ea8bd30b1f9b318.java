package SortListOfStringsInAlphabeticOrder;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> li = new ArrayList<>();
        li.add("Sulav");
        li.add("Binod");
        li.add("Anisha");
        li.add("Marvel");
        li.add("Ishwor");

        li.sort((str1, str2) -> str1.compareToIgnoreCase(str2));
        System.out.println(li);
    }
}