package ru.gb.koval.lesson3_2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class CollectionLesson {
    public static void main(String[] args) {
        //"сырой тип" raw type тк без <...>
        // CЫРОЙ ТИП НЕ ИСПОЛЬЗОВАТЬ!!
        // ArrayList<Integer> list2 = new ArrayList<>(); <- вот так надо
        ArrayList list = new ArrayList(); // ArrayList  класс для создания пустого массива
        //добавление элементов в массив
        list.add("GENRY");
        list.add(new Cat(2, "Pol"));
        list.add(140);
        list.add(16);
        list.add(111);
        System.out.println(list);
        //проверка наличия элемента выдвет тру фолс
        boolean contains = list.contains(140);
        System.out.println(contains);
        //добавленеи эл в массив с укзанием ячейки
        list.add(0, 18);
        System.out.println(list);
        list.add(1, 1);
        System.out.println(list);
        //удаленеи элта с указанием его индекса
        list.remove(0);
        System.out.println(list);
//распечатать элементы массива по порядку
        for (Object o : list) {
            System.out.println(o);
        }
//сортировка от меньшего к большему для цифр
        //  Collections.sort(list);
        System.out.println(list);

        // Collections.много функций
        list.trimToSize();//уменьшение массива до фактической длины массива

        System.out.println();

        //<Integer> массив только из чисел
        ArrayList<Integer> list2 = new ArrayList<>(1000);
        list2.add(140);
        list2.add(16);
        list2.add(111);
        //вывести элемент указав индекс
        Integer integer = list2.get(2);


        LinkedList<String> strings = new LinkedList<>();
        strings.add("a");//добавление в конец
        strings.addFirst("b"); //добавление  в начало
        strings.remove(1);
        System.out.println(strings);


    }
}

class Cat {
    public int age;
    public String name;

    public Cat(int age, String name) {
        this.age = age;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Cat{" +
                "age=" + age +
                ", name='" + name + '\'' +
                '}';
    }
}