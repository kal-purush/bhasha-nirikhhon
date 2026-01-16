package ru.otus.homework.hw11;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        PersonDataBase personDataBase = new PersonDataBase();
        personDataBase.add(new Person("Джон", Position.MANAGER, 1L));
        personDataBase.add(new Person("Джон", Position.DEVELOPER, 10L));
        personDataBase.add(new Person("Джон", Position.QA, 5L));
        personDataBase.add(new Person("Джон", Position.SENIOR_MANAGER, 7L));
        System.out.println(personDataBase);

        var person = personDataBase.findById(5L);
        System.out.println(person);
        var isManager = personDataBase.isManager(new Person("Джон", Position.SENIOR_MANAGER, 10L));
        System.out.println(isManager);
        var isEmployee = personDataBase.isEmployee(10L);
        System.out.println(isEmployee);

        int[] array1 = {77, 1, 14, 6, 7, 98, 4, 2, -9};
        int[] array2 = {3, 1, 11, 0, 54, 15, 7, -44, 87};
        SortArray.bubbleSort(array1);
        System.out.println(Arrays.toString(array1));
        SortArray.quickSort(array2, 0, array2.length - 1);
        System.out.println(Arrays.toString(array2));
    }
}