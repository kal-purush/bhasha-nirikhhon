package lesson4.task4_1;

import lesson4.task4_2.MyArray;

import java.util.ArrayList;

public class PrintElement {

   public void printElement(Object o) {
        System.out.println("Тип: " + o.getClass().getName());
    }

    public void printElement(MyArray o) {
        System.out.println("Элементы массива:");
        for (int i = 0; i < o.getMyData().size(); i++) {
            System.out.printf(" %s", o.getMyData().get(i));
        }
    }

    public void printElement(String o1, String o2) {
        System.out.printf("Максимальное значение: %s \n", o1);
        System.out.printf("Пришли данные: %s \n", o2);
    }

}