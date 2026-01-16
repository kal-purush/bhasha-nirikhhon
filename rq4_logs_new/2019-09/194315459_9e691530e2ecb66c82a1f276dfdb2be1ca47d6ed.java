package com.lern.homeWork.lesson7.arrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainArrayList {

    public static void main(String[] args) {

        String names[] = {"Thomas", "Behemoth", "Alex", "Fiksik", "Philipp", "Pushok", "Snejok", "Simon", "Shark", "Cezar"};
        List<String> name = new ArrayList<>(Arrays.asList(names));
        System.out.println(name.toString());

        System.out.println(name.size());
        System.out.println(name.isEmpty());

        System.out.println(name.contains("Philipp"));
        System.out.println(name.contains("Olia"));

        String[] nameArray = name.toArray(new String[0]);
        System.out.println(Arrays.toString(nameArray));

        System.out.println(name.get(6));

        System.out.println(name.set(2, "Musik"));
        System.out.println(name.toString());

        System.out.println("");
        System.out.println(name.remove(9));
        System.out.println(name.toString());
        System.out.println(name.size());

        System.out.println(name.remove("Pushok"));
        System.out.println(name.toString());
        System.out.println(name.size());

        System.out.println(name.remove("Olia"));
        System.out.println(name.toString());

        System.out.println(name.indexOf("Shark"));
        System.out.println(name.toString());

        System.out.println(name.indexOf("Olia"));
        System.out.println(name.toString());

        System.out.println(name.add("Sasha"));
        System.out.println(name.toString());

        name.add(4, "Sasha");
        System.out.println(name.toString());

        System.out.println(name.size());
        name.lastIndexOf("Sasha");
        System.out.println(name.toString());

        System.out.println(name.indexOf("Sasha"));
        System.out.println(name.toString());

        System.out.println(name.size());
        name.add(10, "Pipi");
        System.out.println(name.toString());

        try {
            System.out.println(name.get(13));
        } catch (RuntimeException badIndex) {
            System.out.println(badIndex.getMessage());
        }
        try {
            System.out.println(name.set(14, "Gusik"));
            System.out.println(name.toString());
        } catch (RuntimeException badIndex) {
            System.out.println(badIndex.getMessage());
        }
        try {
            System.out.println(name.remove(16));
            System.out.println(name.toString());

        } catch (RuntimeException badIndex) {
            System.out.println(badIndex.getMessage());
        }

        name.clear();
        System.out.println(name.isEmpty());
    }
}