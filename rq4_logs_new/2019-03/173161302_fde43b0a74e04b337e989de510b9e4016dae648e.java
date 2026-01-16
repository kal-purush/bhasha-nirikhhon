package lesson3;

import java.util.*;

public class Main3 {
    public static void main(String[] args) {
        String [] fruits = new String[]{"apple", "pineapple", "pear","melon","watermelon","kiwi","mango","orange",
                "lemon","apple","pineapple","pear", "pineapple"};

        Set<String> mySet = new HashSet<String>(Arrays.asList(fruits));

        System.out.println(mySet);

        List<String> myList = new ArrayList<>(Arrays.asList(fruits));
        for (String s:
             mySet) {
            System.out.println(s + " " + Collections.frequency(myList, s));
        }

        System.out.println("----");

        PhoneBook myPhoneBook = new PhoneBook();
        myPhoneBook.add("Ivanov", 2633737);
        myPhoneBook.add("Petrov", 6438035);
        myPhoneBook.add("Sidorov", 5020323);
        myPhoneBook.add("Smirnov", 5320324);
        myPhoneBook.add("Ivanov", 3242393);

        myPhoneBook.print();

        System.out.println("------");
        myPhoneBook.get("Ivanov");


    }






}