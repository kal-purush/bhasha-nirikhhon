package leetCode;

import java.util.LinkedList;

/**
 * Вам даны заголовки двух отсортированных связанных списков list1 и list2.
 * Объедините два списка в один отсортированный список.
 * Список должен быть составлен путем соединения узлов первых двух списков.
 * Возвращает заголовок объединенного связанного списка.
 */
public class MergeTwoSortedLists {
    public static void main(String[] args) {
        // создали 2 листа
        LinkedList<Integer> list1 = new LinkedList<>();
        list1.add(22);
        list1.add(11);
        list1.add(77);
        list1.add(32);
        LinkedList<Integer> list2 = new LinkedList<>();
        list2.add(12);
        list2.add(90);
        list2.add(55);
        list2.add(88);

        //обьеденяем 2 листа вместе
        LinkedList<Integer> mergeList = mergeSortedLists(list1, list2);

        System.out.println("Обьедененный связной список: " + mergeList);

    }
     //создали метод который на вход принимает 2 листа
    public static LinkedList<Integer> mergeSortedLists (LinkedList<Integer> list1, LinkedList<Integer> list2){
        LinkedList<Integer> mergeList = new LinkedList<>();

        while (!list1.isEmpty() && ! list2.isEmpty()) { //проверка листа на пустоту
            if (list1.peek()< list2.peek()) { //получаем значение каждого элемента из головы каждого списка без удаления
                mergeList.add(list1.poll());
            }else {
                mergeList.add(list2.poll());
            }
        }

        //Добавляем оставшиеся элементы из первого списка
        while (!list1.isEmpty()) {
            mergeList.add(list1.poll());
        }

        while (!list2.isEmpty()) {
            mergeList.add(list2.poll());
        }

        return mergeList;
    }

}

