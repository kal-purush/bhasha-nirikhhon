package lesson15.task3;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Toy {
    public static void main(String[] args) {
        Map<String, Double> toys = new HashMap<>();
        toys.put("Ball", 100.4);
        toys.put("Bicycle", 10000.53);
        toys.put("Robot", 111.65);
        toys.put("Doll", 525.65);

        Set<String> set1 = toys.keySet();
        for (String toy1 : set1) {
            System.out.println(toy1 + " " + toys.get(toy1));
        }
        System.out.println();
        Set<Map.Entry<String, Double>> set = toys.entrySet();
        for (Map.Entry<String, Double> toy : set) {
            System.out.print(toy.getKey() + " : ");
            System.out.println(toy.getValue() + ".");
        }
        System.out.println();
        Collection<Double> set2 = toys.values();
        for (Double toy2 : set2) {
            System.out.println(toy2);
        }
    }
}