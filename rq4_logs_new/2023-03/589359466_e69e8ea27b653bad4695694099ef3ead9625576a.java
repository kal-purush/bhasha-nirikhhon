package com.ua.robotdreams.homework18;

public class Main {
    public static void main(String[] args) {
        Random random = new Random();
        Set<Integer> hs = new HashSet<>();
        Set<Integer> lhs = new LinkedHashSet<>();
        Set<Integer> ts = new TreeSet<>();

        for (int i = 0; i < 1000; i++) {
            hs.add(random.nextInt(1, 51));
            lhs.add(random.nextInt(1, 51));
            ts.add(random.nextInt(1, 51));
        }

        System.out.println(hs);
        System.out.println(lhs);
        System.out.println(ts);
    }
}