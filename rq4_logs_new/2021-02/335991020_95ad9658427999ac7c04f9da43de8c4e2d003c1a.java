package GeekBrians.Slava_5655380.Homework;

public class Lesson_5 {
    public static void main(String[] args) {
        Dog dogBobik = new Dog("Бобик"), dogSharik = new Dog("Шарик"), dogBarboskin = new Dog("Барбоскин");
        Cat catVaska = new Cat("Васька"), catPushok = new Cat("Пушок"), catTom = new Cat("Том");
        System.out.println("Созданно " + Animals.getCounter() + " животных.");
        // ЗАДАНИЕ 2. Все животные могут бежать и плыть. В качестве параметра каждому методу передается длина препятствия.
        // Результатом выполнения действия будет печать в консоль. (Например, dogBobik.run(150); -> 'Бобик пробежал 150 м.');
        dogBobik.run(150);
        dogBobik.swim(6);
        catTom.run(1000);
        catTom.swim(100);
        catTom.die();
        System.out.println("Осталось " + Animals.getCounter() + " животных.");
    }
}

// ЗАДАНИЕ 1. Создать классы Собака и Кот с наследованием от класса Животное.
class Animals {
    // ЗАДАНИЕ 4. Добавить подсчет созданных котов, собак и животных.
    protected static int counter = 0;
    protected String name;

    Animals() {
        name = "Безымянный";
        counter++;
    }

    Animals(String name) {
        this.name = name;
        counter++;
    }

    public void run(int length) {
        System.out.print(name + " пробежал " + length + " м.\n");
    }

    public void swim(int length) {
        System.out.print(name + " проплыл " + length + " м.\n");
    }

    void die() {
        counter--;
    }

    public static int getCounter() {
        return counter;
    }
}

class Dog extends Animals {
    public final int MAX_RUN_LENGTH = 500;
    public final int MAX_SWIM_LENGTH = 10;

    public Dog() {
        super();
    }

    public Dog(String name) {
        super(name);
    }

    @Override
    public void run(int length) {
        // ЗАДАНИЕ 3. У каждого животного есть ограничения на действия (бег: кот 200 м., собака 500 м.; плавание: кот не умеет плавать, собака 10 м.).
        if (length > MAX_RUN_LENGTH)
            System.out.print(name + " не смог пробежать " + length + " м. Но ");
        super.run((length < MAX_RUN_LENGTH) ? length : MAX_RUN_LENGTH);
    }

    @Override
    public void swim(int length) {
        if (length > MAX_SWIM_LENGTH)
            System.out.print(name + " не смог проплыть " + length + " м. Но ");
        super.swim((length < MAX_SWIM_LENGTH) ? length : MAX_SWIM_LENGTH);
    }

}

class Cat extends Animals {
    public final int MAX_RUN_LENGTH = 200;

    public Cat() {
        super();
    }

    public Cat(String name) {
        super(name);
    }

    @Override
    public void run(int length) {
        if (length > MAX_RUN_LENGTH)
            System.out.print(name + " не смог пробежать " + length + " м. Но ");
        super.run((length < MAX_RUN_LENGTH) ? length : MAX_RUN_LENGTH);
    }

    @Override
    public void swim(int length) {
        System.out.print(name + " не смог проплыть " + length + " м.\n");
    }
}