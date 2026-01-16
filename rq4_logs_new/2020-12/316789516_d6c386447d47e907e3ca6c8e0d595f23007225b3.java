package hw3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Main {
    public static void main(String[] args) {
        // Task 1
        ArrayList<String> words = addWords();
        HashSet<String> wordsSet = new HashSet<>(words.size());
        HashMap<String, Integer> wordsMap = new HashMap<>(words.size());

        for (int i = 0; i < words.size(); i++) {
            String key = words.get(i);
            wordsSet.add(key);

            if (wordsMap.containsKey(key))
                wordsMap.put(key, wordsMap.get(key) + 1);
            else
                wordsMap.put(key, 1);
        }

        System.out.println("Task 1");
        System.out.println("Words in text:\n " + wordsSet);
        System.out.println("Words quantity:\n" + wordsMap);

        // Task 2
        PhoneBook pb = new PhoneBook();
        pb.add(new Person("Ivanov", "7355608", "ivanov@gmail.com"));
        pb.add(new Person("Ovchinnikov", "123321123", "thebestteacher@gmail.com"));
        pb.add(new Person("Petrov", "+79995347940", "garvardinho@mail.ru"));
        pb.add(new Person("Ivanov", "153306", "ivanov22@gmail.com"));
        pb.add(new Person("Ivanov", "153306", "wontbeadded@gmail.com"));
        pb.add(new Person("Ivanov", "153308", "qwerty@gmail.com"));
        pb.add(new Person("Petrov", "7355608", "wontbeadded@gmail.com"));
        pb.add(new Person("Petrov", "456789", "Petrov@gmail.com"));

        System.out.println("\nTask 2");
        System.out.println(pb);
        System.out.println(pb.getPhone("Ivanov"));
        System.out.println(pb.getEmail("Petrov"));
    }

    public static ArrayList<String> addWords() {
        ArrayList<String> words = new ArrayList<>(30);

        words.add("package");
        words.add("hw3");
        words.add("import");
        words.add("java");
        words.add("util");
        words.add("ArrayList");
        words.add("public");
        words.add("class");
        words.add("Main");
        words.add("main");
        words.add("public");
        words.add("static");
        words.add("void");
        words.add("main");
        words.add("ArrayList");
        words.add("String");
        words.add("words");
        words.add("new");
        words.add("ArrayList");
        words.add("main");
        words.add("addWords");
        words.add("static");
        words.add("public");
        words.add("Main");
        words.add("ArrayList");
        words.add("public");
        words.add("static");
        words.add("public");
        words.add("public");
        words.add("main");
        words.add("static");

        return words;
    }
}