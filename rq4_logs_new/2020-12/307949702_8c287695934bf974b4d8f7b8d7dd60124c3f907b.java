import java.util.*;

public class MainClass {

    public static void main(String[] args) {
//        1. Создать массив с набором слов (10-20 слов, должны встречаться повторяющиеся).
//                Найти и вывести список уникальных слов, из которых состоит массив (дубликаты не считаем).
//                Посчитать сколько раз встречается каждое слово.
        String[] words = {"apple", "orange", "lemon", "banana", "apricot",
                "avocado", "broccoli", "carrot", "cherry", "garlic",
                "grape", "melon", "leak", "kiwi", "mango",
                "avocado", "broccoli", "orange", "lemon", "banana"};

        Set<String> wordsList = new TreeSet<>(Arrays.asList(words));
        System.out.println(wordsList);  // список уникальных слов, из которых состоит массив (дубликаты не считаем)

        HashMap<String, Integer> freqWords = new HashMap<>();
        for (String str : wordsList) {
            freqWords.put(str, Collections.frequency(Arrays.asList(words), str));
        }

        System.out.println(freqWords); //сколько раз встречается каждое слово.

//        2. Написать простой класс ТелефонныйСправочник, который хранит в себе список фамилий и телефонных номеров.
//        В этот телефонный справочник с помощью метода add() можно добавлять записи. С помощью метода get() искать
//        номер телефона по фамилии. Следует учесть, что под одной фамилией может быть несколько телефонов
//                (в случае однофамильцев), тогда при запросе такой фамилии должны выводиться все телефоны.

        Phonebook pB = new Phonebook();
        pB.add("Ivanov", "+79098431515");
        pB.add("Petrov", "+79991512020");
        pB.add("Sidorov", "+79151310799");
        pB.add("Ivanov", "+79111234565");
        pB.add("Krylov", "+79754651122");
        pB.add("Proschenko", "+79691171089");
        pB.add("Fits", "+79098431704");
        pB.add("Fits", "+79150975528");
        pB.add("Fits", "+79098714737");


        pB.printBook();
        pB.get("Fits");
        pB.get("Krylov");
    }
}