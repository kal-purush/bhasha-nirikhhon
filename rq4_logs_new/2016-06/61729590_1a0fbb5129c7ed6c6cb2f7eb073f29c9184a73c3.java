package cz.mikealdo.mt;

public class Person {

    int age;

    public Person(int age) {
        this.age = age;
    }

    public boolean isInRetirement() {
        return age > 60 && age < 80;
    }
}