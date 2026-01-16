package chapter6.sandbox;

import java.util.HashMap;
import java.util.Map;

public class Sandbox {
    public static void main(String[] args) {
        String[] words = { "abc", "abc", "bdc", "a", "x", "y", "z", "z", "y", "abc" };
        Map<String, Integer> wordCounter = new HashMap<>();
        for (String word : words) {
            wordCounter.compute(word, (k, v) -> v == null ? 1 : v + 1);
        }

        for (Map.Entry<String, Integer> e : wordCounter.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }
    }
}