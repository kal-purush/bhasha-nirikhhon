package Anonimus_class_57_58_d;



import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Main {
    public static void main(String[] args)  {
        //Анонимный класс
        Printer printer = new Printer() {
            @Override
            public void print(String message) {
                System.out.println("1) Ответить на Сообщение: Анонимный класс");
            }
        };printer.print("Анонимный класс");

        //1) лямбда выражение.
      Printer printer1 = message -> System.out.println("2) Ответить на Сообщение: лямбда выражение");
        printer1.print("лямбда выражение");

        //2) Блочное лямбда выражение.
        Printer printer2 = message -> {
            System.out.println(message);
            System.out.println("3) Длинна сообщения " + (message.length()));
        };printer2.print("Блочное лямбда выражение");


        //Сортировка списка строк с использованием Comparator
        List<String> names = Arrays.asList("Платон","Александр","Иван","Василий", "Эдуард");
        Comparator<String> comparator = new Comparator<>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.compareTo(o1);
            }
        };
        Collections.sort(names,comparator);
        System.out.println(names);
        Collections.sort(names, (n1,n2) -> n2.compareTo(n1));
        System.out.println(names);

        //StreamApi
        List<String> words = Arrays.asList("apple", "banana", "fig", "date", "kiwi", "grape");

        List<String> words1 = words.stream()
                .filter(w -> w.length() > 4)
                .sorted(Comparator.comparingInt(String::length))
                .map((w) -> w.toUpperCase())
                .peek(w -> System.out.println(w))
                .toList();
    }
}