package lesson_14_Collections_Framework.examples.list;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

// Якщо під час обробки списку (тут, проходження по списку)
// втручається певна дія над цим списком (тут, видалення елементу),
// то може бути викинутий виняток ConcurrentModificationException.
// Цей виняток може бути створено методами, які виявили одночасну модифікацію
// об’єкта (тут, типу List), коли така модифікація неприпустима.
// Наприклад, загалом заборонено, щоб один потік змінював колекцію, поки інший
// потік виконує її.
// https://docs.oracle.com/javase/8/docs/api/java/util/ConcurrentModificationException.html
// Виняток ConcurrentModificationException торкається теми багатопотоковості
// (розглядається окремо).
public class ConcurrentModificationException_Example {

    public static void main(String[] args) {
        // Створення списку через конструктор ArrayList
        List<String> list = new ArrayList<>();
        // Додавання елементів до списку
        list.add("orange");
        list.add("banana");
        list.add("mango");
        list.add("kiwi");
        // Виведення списку
        System.out.println("\n1) DATA:");
        for (String item : list) {
            System.out.println(item);
        }

        // Намагаємося пройти списком, одночасно видаляючи
        // певний елемент. Маємо обробити можливий виняток.
        System.out.println("\n2) DATA:");
        try {
            for (String item : list) {
                list.remove(2);
                System.out.println(item);
            }
        } catch (ConcurrentModificationException e) {
            System.out.println("Exception: " + e.getMessage());
        }

        // Елегантне рішення через Collection.removeIf,
        // для запобігання ConcurrentModificationException.
        list.removeIf(
                // Лямбда-вираз, який стосується галузі
                // функціонального програмування (розглядається окремо).
                item -> item.equals("mango")
        );
        // Виведення списку
        System.out.println("\n3) DATA:");
        for (String item : list) {
            System.out.println(item);
        }
    }
}