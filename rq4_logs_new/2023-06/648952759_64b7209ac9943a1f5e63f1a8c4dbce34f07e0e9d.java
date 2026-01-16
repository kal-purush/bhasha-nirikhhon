/*
 * Приватное поле "имя" (name) типа String для хранения имени кота.
Приватное поле "возраст" (age) типа int для хранения возраста кота.
Публичные методы доступа (геттеры и сеттеры) для полей "имя" и "возраст".
Публичный метод "приветствие" (greet), который выводит на консоль приветствие вида "Мяу! 
Меня зовут <имя>. Мне <возраст> года(лет).".
 * 
 */
package lesson1;

public class Cat {
    private String name;
    private int age;
    private Owner owner;

    public Cat(String name, int age, String owner) {
        this.name = name;
        this.age = age;
        this.owner = new Owner(owner);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void greet() {
        System.out.printf("Мяу! Меня зовут %s. Мне %s года(лет). "
                + "Мой владелец %s", this.name, this.age, this.owner);
    }
}