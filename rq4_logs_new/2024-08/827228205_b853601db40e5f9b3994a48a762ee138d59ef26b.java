package ru.otus.java.basic.safarov.homework11;

public class Person {
    private final String name;
    private Position position;
    private final Long id;
//    private static Long cntId = 0L;

    public Person(String name, Long id) {
        this.name = name;
        this.id = id;
//        id = ++cntId;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public String getName() {
        return name;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        return  name + " (id: " + id  + ") должность " + position;
    }
}