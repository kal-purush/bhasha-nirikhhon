package leetCode;

import java.util.HashMap;


public class HashMapContains {


    public static void main(String[] args) {
        HashMap<String, Integer> h = new HashMap<>();
        h.put("Nick", 34);
        h.put("Kate", 32);
        h.put("Anni", 31);
        h.put("Henrick", 22);


        // Прверим содержит ли наша мапа ключ "Kyk"
        boolean containsKyk = h.containsKey("Kyk");
        System.out.println("Contains Kyk " + containsKyk);

        //прверка мапы на пустоту
        boolean isEmpty = h.isEmpty();
        System.out.println("HashMap " + isEmpty);

        Integer removeKey = h.remove("Nick");
        System.out.println("Remove Key " + removeKey);


        // Прверим содержит ли наша мапа ключ "Nick"
        boolean containsNick = h.containsKey("Nick");
        System.out.println("Contains Nick " + containsNick);
    }


}