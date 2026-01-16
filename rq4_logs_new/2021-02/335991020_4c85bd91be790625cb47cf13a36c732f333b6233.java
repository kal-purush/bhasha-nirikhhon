package GeekBrians.Slava_5655380.Homework.Lesson10;

import java.util.*;

public class MainDemo {
    public static void main(String[] args) {
        //task1();
        task2();
    }

    public static void task1() {
        // ЗАДАНИЕ 1.
        // Создать массив с набором слов (10-20 слов, должны встречаться повторяющиеся).
        String srcStr = "Кроваво-черное ничто пустилось вить систему клеток, связанных внутри, клеток, связанных внутри, клеток в едином стебле и явственно, " +
                "до жути на фоне тьмы ввысь белым бил фонтан. Клетки. Клетки. Клетки. Связаны. Связаны. Связаны.";
        String[] wordArr = srcStr.split(" |, |\\. |\\.");
        for (int i = 0; i < wordArr.length; i++)
            wordArr[i] = wordArr[i].toUpperCase();
        System.out.println("Массив слов:");
        for (String word : wordArr)
            System.out.print(word + " ");

        HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
        for (String word : wordArr)
            wordMap.put(word, (wordMap.get(word) == null) ? 1 : wordMap.get(word) + 1);
        // Найти и вывести список уникальных слов, из которых состоит массив (дубликаты не считаем).
        System.out.println("\n----------------------------------");
        System.out.println("Уникальные слова: ");
        for (String word : wordMap.keySet())
            System.out.print(word + " ");
        // Посчитать, сколько раз встречается каждое слово.
        System.out.println("\n----------------------------------");
        System.out.println("Частота слов");
        for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    public static void task2() {
        TeleDir teleDB = new TeleDir();

        teleDB.add("Йцукен", 88005553535l);
        teleDB.add("Шотим", 89991118888l);
        teleDB.add("Йцукен", 89851408080l);
        teleDB.add("Лорпав", 89851408080l);
        teleDB.add("Кузнецов", 87001001010l);

        for (var iter = teleDB.iterator(); iter.hasNext(); ) {
            String key = iter.next().getKey();
            LinkedList<Long> value = teleDB.get(key);
            System.out.println(key + ((value.size() > 1) ? "(" + value.size() + ")" : "") + ": " + teleDB.get(key));
        }
    }
}