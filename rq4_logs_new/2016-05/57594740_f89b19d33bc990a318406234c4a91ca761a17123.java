package lesson.lesson_07;

import lesson.lesson_07.person.Person; // ������ ������

/**
 * Created by admin on 22.05.2016.
 */
public class PersonMain {
    public static void main(String[] args) {
        Person person = new Person("Ivan", "Ivanov", "89231232463");
        person.print();

        System.out.println(person.getName());
    }
}