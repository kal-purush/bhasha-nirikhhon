package homework.day11;

import homework.day11.objects.Chair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyChairs {
    public static void main(String[] args) {
        List<Chair> chairs = Arrays.asList(new Chair(1, 2), new Chair(4, 2), new Chair(2, 4));

        for (Chair chair : chairs) {
            System.out.print(chair.getHeight() * chair.getWidth() + " ");
        }
        System.out.println();

        Map<Integer, Chair> myMap = new HashMap<>();
        for (int i = 0; i < chairs.size(); i++) {
            myMap.put(i, chairs.get(i));
        }

        for (int key : myMap.keySet()) {
            System.out.println(key);
        }

        for (Chair value : myMap.values()) {
            System.out.println(value);
        }

        for (Map.Entry<Integer, Chair> key : myMap.entrySet()) {
            System.out.println(key);
        }
    }
}