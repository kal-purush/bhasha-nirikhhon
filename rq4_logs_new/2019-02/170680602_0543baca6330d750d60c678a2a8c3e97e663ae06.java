package lesson3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 2. Написать простой класс ТелефонныйСправочник, который хранит в себе список фамилий и телефонных номеров.
// В этот телефонный справочник с помощью метода add() можно добавлять записи. С помощью метода get() искать
// номер телефона по фамилии. Следует учесть, что под одной фамилией может быть несколько телефонов
// (в случае однофамильцев), тогда при запросе такой фамилии должны выводиться все телефоны.
class PhoneBook {
    private Map<String,String> pb = new HashMap<String, String>();

    public void add (String familyName, String phoneNumber) {
        String phone = pb.get(familyName);
        if (phone == null) pb.put(familyName, phoneNumber);
        else pb.replace(familyName, phone+", "+phoneNumber);
    }

    public String get(String familyName) {
        return pb.get(familyName);
    }

//    public void output(){
//        for (Map.Entry<String,String> m: pb.entrySet()) {
//            System.out.println(m.getKey() + " - " + m.getValue());
//        }
//    }
}

public class lesson3 {
    public static void main(String[] args) {
        // 1. Создать массив с набором слов (10-20 слов, должны встречаться повторяющиеся).
        // Найти и вывести список уникальных слов, из которых состоит массив (дубликаты не считаем).
        // Посчитать сколько раз встречается каждое слово.
        String[] dictArray = new String[]{"слон", "мартышка", "дикобраз", "попугай", "питон", "слон", "чайка", "тигр", "шакал", "мартышка",
        "гиена", "павлин", "слон", "мартышка", "попугай", "коршун", "антилопа", "мартышка", "тигр", "антилопа"};
        List<String> dictionary = Arrays.asList(dictArray);
        Map<String,Integer> dictMap = new HashMap<String,Integer>(dictionary.size());

        for (String word: dictionary) {
            Integer count = dictMap.get(word);
            dictMap.put(word, count == null ? 1 : count+1);
        }
        //System.out.println(dictMap);

        // Тестирование класса PhoneBook
        PhoneBook pb = new PhoneBook();
        pb.add("Ivanov", "+78901234567");
        pb.add("Petrov", "+7 123 456-78-90");
        pb.add("Sidorov","+7(901)2345678");
        pb.add("Ivanov","8-910-293-84-75");
        System.out.println("Ivanov : "+pb.get("Ivanov"));
        System.out.println("Petrov : "+pb.get("Petrov"));
    }
}