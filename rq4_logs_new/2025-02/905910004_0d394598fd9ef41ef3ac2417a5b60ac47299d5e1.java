package lesson_15_Map_Interface;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashMapMain {
    public static void main(String[] args) {
        Set<Integer> hashSet = new HashSet<>();
        hashSet.add(10);

        Map<String, Double> hm = new HashMap<>();
        hm.put("John Doe", 3434.34);
        hm.put("Tom Smith", 123.22);
        hm.put("Jane Baker", 1378.00);
        hm.put("Tod Hall", 99.22);
        hm.put("Ralph Smith", -19.08);
        hm.put("Roy Sommers", null);
        hm.put("Benny Hill", null);

        Set<Map.Entry<String,Double>> set = hm.entrySet();

        for (Map.Entry<String,Double> sm : set) {
            System.out.println(sm.getKey() + ": ");
            System.out.println(sm.getValue());
        }

        System.out.println();
        double balance = hm.get("John Doe");
        hm.put("John Doe", balance + 1000);
        System.out.println("New balance of John Doe: " + hm.get("John Doe"));

        Map<Integer, String> map2 = new HashMap<>(16,0.75F); // Дефолтные значения для Hashmap.
    }
}