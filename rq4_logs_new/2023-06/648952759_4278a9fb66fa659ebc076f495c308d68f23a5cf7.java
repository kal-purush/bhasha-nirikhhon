/*
 * Класс очередь. Создет очередь, добавляет покупателей эту очередь. 
 * В будущем очередей может быть несколько.
 */

package lesson2;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Random;

public class MyQueue extends A_Deque implements I_QueueBehaviour {

    public MyQueue() {
        System.out.println("Очередь подготовлена");
        System.out.println();
    }

    @Override
    public void addInQueue(Сustomer cСustomer) {
        queue.add(cСustomer);
    }

    @Override
    public void delOutQueue() {
        queue.removeFirst();
    }

    @Override
    public void mouveQueue() {
        Random random = new Random(); // для случайного выбора покупателей
        int temp;
        int i = 0; // временный индекс
        Deque<Сustomer> queueTemp = new LinkedList<>();
        temp = random.nextInt(queue.size());
        for (Сustomer сustomer : queue) {
            if (i != temp) {
                queueTemp.addLast(сustomer);
            } else {
                System.out.printf("%s не дождался и покинул очередь",
                        сustomer.getNameCustomer());
                System.out.println();
            }
            i++;
        }
        queue.clear();
        queueTemp.stream().forEach(S -> queue.addLast(S));
    }

    @Override
    public String getLast() {
        return queue.getLast().getNameCustomer();
    }

    @Override
    public String getQueue() {
        String allQueue = "";
        for (Сustomer сustomer : queue) {
            allQueue += сustomer + "; ";
        }
        return allQueue;
    }

    @Override
    String getFirst() {
        return queue.getFirst().getNameCustomer();
    }

}