package Lesson4;

import Lesson4.gb.GBList;
import Lesson4.gb.list.GBArrayList;

import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        GBList<Integer> gbList = new GBArrayList<>();
        gbList.add(1);
        gbList.add(2);
        gbList.add(3);
        gbList.add(4);

//        System.out.println(gbList.get(0));
//        System.out.println(gbList.get(2));

        for (Integer el : gbList) System.out.print(el + " ");
        System.out.println();

        System.out.println(gbList.size());
        gbList.remove(gbList.size() - 1);

        for (Integer el : gbList) System.out.print(el + " ");
        System.out.println();

        System.out.println(gbList.size());

    }
}