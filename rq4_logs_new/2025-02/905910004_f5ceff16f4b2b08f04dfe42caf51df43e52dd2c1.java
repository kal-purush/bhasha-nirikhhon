package lesson_12_Multithreading.theory.atomic_variables;

import java.util.concurrent.atomic.AtomicInteger;

// Метод addAndGet(int delta) атомарно добавляет
// заданное значение для текущего значения.
// Возвращает обновленное значение.
public class AddAndGet_MethodExample {

    public static void main(String[] args) {
        // Можем установить значение в конструкторе
        AtomicInteger val = new AtomicInteger(2);
        System.out.println("Initial value is " + val);
        int res = val.addAndGet(5);
        System.out.println("Updated value is " + res);
    }
}

// Результат работы кода:

// Initial value is 2
// Updated value is 7