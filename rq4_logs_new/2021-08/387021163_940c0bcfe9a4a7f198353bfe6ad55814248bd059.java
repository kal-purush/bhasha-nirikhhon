package Lessons7;

class Lessons6 {
    public static void main(String[] args) {
        Cat cat = new Cat();
        Cat cat2 = new Cat();
        Dog dog = new Dog();
        cat.swimm(5);
        cat2.run(150);
        dog.swimm(7);


       Cat [] catArray = {new Cat("Васёк"), new Cat("Петька"), new Cat ("Леброн")};
        Plate plate = new Plate(40);
        for (Cat elem : catArray) {
           elem.eating(15, plate);
        }
        for (Cat elem : catArray) {
            System.out.println("Кот по имени " + elem.getName() + (elem.isFull()?" сытый": " голодный"));
        }



//        int a = Animal.getCount();

        System.out.print("животных создано: " + Animal.getCount());

    }
}

