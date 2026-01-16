package GeekBrians.Slava_5655380.Homework;

import java.math.BigInteger;

public class Lesson_4 {
    public static void main(String[] args) {
        // ЗАДАНИЕ 4. Создать массив из 5 сотрудников.
        Employee[] employees = {
                new Employee(
                        "Пупкин Василий Петрович",
                        "CEO",
                        "pupkin.va@sharashka.com",
                        78005553535L,
                        400000,
                        49
                ),
                new Employee(
                        "Пупкина София Срегеевна",
                        "HR",
                        "pupkin.ss@sharashka.com",
                        78001233212L,
                        300000,
                        22

                ),
                new Employee(
                        "Йцукен Енгош Лорпав",
                        "Graphic Designer",
                        "qwerty.tk@sharashka.com",
                        79853211234L,
                        20000,
                        24
                ),
                new Employee(
                        "Каценеленбоген Абрам Яковлевич",
                        "Sales manager",
                        "Katselenbogen.ay@sharashka.com",
                        79851231234L,
                        250000,
                        87
                        ),
                new Employee(
                        "Пупкин Пётр Васильевич",
                        "Creative director",
                        "pupkin.pv@sharashka.com",
                        78003211212L,
                        300000,
                        19
                )
        };

        //ЗАДАНИЕ 5. С помощью цикла вывести информацию только о сотрудниках старше 40 лет.
        for(var employee : employees)
            if(employee.getAge() > 40)
                System.out.println(employee);
    }
}
// ЗАДАНИЕ 1. Создать класс "Сотрудник" с полями: ФИО, должность, email, телефон, зарплата, возраст.
class Employee{
    private String name;
    private String position;
    private String email;
    private long phoneNumber;
    private int salary;
    private short age;

    // ЗАДАНИЕ 2. Конструктор класса должен заполнять эти поля при создании объекта.
    public Employee(String name, String position, String email, long phoneNumber, int salary, int age) {
        this.name = name;
        this.position = position;
        this.email = email;
        this.phoneNumber = phoneNumber;
        this.salary = salary;
        this.age = (short)age;
    }

    // ЗАДАНИЕ 3. Внутри класса «Сотрудник» написать метод, который выводит информацию об объекте в консоль.
    @Override
    public String toString() {
        return "Сотрудник {\n   ФИО: " + name +
                "\n   ДОЛЖНОСТЬ: " + position +
                "\n   E-MAIL: " + email +
                "\n   ТЕЛЕФОН: +" + phoneNumber +
                "\n   ЗАРПЛАТА: " + salary + "₽" +
                "\n   ВОЗРАСТ: " + age +
                "\n}\n";
    }

    public short getAge() {
        return age;
    }
}