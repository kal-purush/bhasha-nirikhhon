package ru.otus.homework.hw10;

import java.util.*;

public class PhoneBook {
    Map<String, List<String>> list = new HashMap<>();

    public PhoneBook() {
    }

    public void add(String name, String phoneNumber) {
        if (list.containsKey(name)) {
            var phoneNumbers = list.get(name);
            phoneNumbers.add(phoneNumber);
            list.put(name, phoneNumbers);
        } else {
            List<String> phoneNumbers = new ArrayList<>();
            phoneNumbers.add(phoneNumber);
            list.put(name, phoneNumbers);
        }
    }

    public void find(String name) {
        if (list.containsKey(name)) {
            var phoneNumbers = list.get(name);
            System.out.println("По имени " + name + " найдены номера телефонов " + phoneNumbers);
        } else {
            System.out.println("По имени " + name + " не найдено ни одного номера телефона в справочнике");
        }
    }

    public boolean containsPhoneNumber(String phoneNumber) {
        Set<Map.Entry<String, List<String>>> entries = list.entrySet();
        for (var pair : entries) {
            List<String> phonesNumbers = pair.getValue();
            for (String phoneNum : phonesNumbers) {
                if (phoneNum.contains(phoneNumber)) {
                    System.out.println("Номер телефона " + phoneNumber + " есть в справочнике");
                    return true;
                }
            }
        }
        System.out.println("Такого номера телефона нет в справочнике");
        return false;
    }

    @Override
    public String toString() {
        return "PhoneBook{" +
                "list=" + list +
                '}';
    }
}