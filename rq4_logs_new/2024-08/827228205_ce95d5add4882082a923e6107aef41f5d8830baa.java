package ru.otus.java.basic.safarov.homework10;

public class Person {
    private final String name;
    private final String surname;
    private final String patronymic;

    public Person(String name, String surname, String patronymic) {
        this.name = name;
        this.surname = surname;
        this.patronymic = patronymic;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getPatronymic() {
        return patronymic;
    }

    @Override
    public String toString() {
        return String.join(" ", surname, name, patronymic);
    }

    public String fio(){
        return surname + " " + name.charAt(0) + "." + patronymic.charAt(0) + ".";
    }
}