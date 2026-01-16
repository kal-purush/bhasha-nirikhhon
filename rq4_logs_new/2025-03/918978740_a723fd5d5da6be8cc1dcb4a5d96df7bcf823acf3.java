package ru.otus.homework.hw09;

import java.util.*;

public class Main {
    public static void main(String[] args) {
        printArray(5, 10);
        sumElements(new ArrayList<>(Arrays.asList(1, 6, 9, 4)));
        updateElements(1, new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        increaseElements(2, new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));
        listOfNames(new ArrayList<>(Arrays.asList(new Employee("Евгений", 35), new Employee("Александр", 45), new Employee("Антон", 40))));
        listOfEmployees(new ArrayList<>(Arrays.asList(new Employee("Евгений", 35), new Employee("Александр", 45), new Employee("Антон", 40))), 40);
        checkAge(new ArrayList<>(Arrays.asList(new Employee("Евгений", 35), new Employee("Александр", 45), new Employee("Антон", 40))), 20);
        findYoungEmployee(new ArrayList<>(Arrays.asList(new Employee("Евгений", 35), new Employee("Александр", 45), new Employee("Антон", 40))));
    }

    public static List<Integer> printArray(int min, int max) {
        List<Integer> list = new ArrayList<>();
        for (int i = min; i <= max; i++) {
            list.add(i);
        }
        return list;
    }

    public static int sumElements(List<Integer> elements) {
        int sum = 0;
        for (var element : elements) {
            if (element > 5) {
                sum += element;
            }
        }
        return sum;
    }

    public static void updateElements(int num, List<Integer> list) {
        for (int i = 0; i < list.size(); i++) {
            list.set(i, num);
        }
    }

    public static void increaseElements(int num, List<Integer> list) {
        for (int i = 0; i < list.size(); i++) {
            list.set(i, list.get(i) + num);
        }
    }

    public static List<String> listOfNames(List<Employee> employees) {
        List<String> names = new ArrayList<>();
        for (var employee : employees) {
            names.add(employee.getName());
        }
        return names;
    }

    public static List<Employee> listOfEmployees(List<Employee> employees, int minAge) {
        List<Employee> list = new ArrayList<>();
        for (var employee : employees) {
            if (employee.getAge() >= minAge) {
                list.add(employee);
            }
        }
        return list;
    }

    public static boolean checkAge(List<Employee> employees, int minAge) {
        int averageAge;
        int allAge = 0;
        for (var employee : employees) {
            allAge += employee.getAge();
        }
        averageAge = allAge / employees.size();
        return averageAge > minAge;
    }

    public static Employee findYoungEmployee(List<Employee> employees) {
        Employee minAgeEmployee = employees.get(0);
        for (var employee : employees) {
            minAgeEmployee = minAgeEmployee.getAge() < employee.getAge() ? minAgeEmployee : employee;
        }
        return minAgeEmployee;
    }
}