package ru.otus.java.basic.safarov.homework10;

import java.util.*;

public class PhoneBook {
    private Map<String, Set<String>> phonebook;

    public PhoneBook() {
        this.phonebook = new HashMap<>();
    }

    public void add(String name, String number){
        if (name.isEmpty() || number.isEmpty()){
            return;
        }
        if (phonebook.containsKey(name)){
            phonebook.get(name).add(number);
        } else {
            Set<String> numbers = new HashSet<>();
            numbers.add(number);
            phonebook.put(name, numbers);
        }
    }

    public String find(String name){
        if(phonebook.containsKey(name)){
//            return phonebook.get(name).toString();
            return (phonebook.get(name)).toString();
        }
        return null;
    }

    public boolean containsPhoneNumber(String number){
        Collection<Set<String>> values = phonebook.values();
        for (Set<String> items: values){
            if (items.contains(number)){
                return true;
            }
        }
        return false;
    }

    public Map<String, Set<String>> getPhonebook() {
        return phonebook;
    }
}