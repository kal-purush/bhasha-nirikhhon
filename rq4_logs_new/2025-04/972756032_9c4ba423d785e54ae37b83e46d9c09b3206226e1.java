// 1. Single Inheritance: Dog inherits from Animal
class Animal {
    void speak() {
        System.out.println("Animal speaks");
    }
}

class Dog extends Animal {
    void bark() {
        System.out.println("Dog barks");
    }
}

// 2. Multilevel Inheritance: Pup inherits from Dog which inherits from Animal
class Pup extends Dog {
    void weep() {
        System.out.println("Pup weeps");
    }
}

// 3. Hierarchical Inheritance: Cat and Dog inherit from Animal
class Cat extends Animal {
    void meow() {
        System.out.println("Cat meows");
    }
}

// 4. Multiple Inheritance (through Interfaces in Java)
interface Student {
    void study();
}

interface Athlete {
    void train();
}

class ScholarAthlete implements Student, Athlete {
    public void study() {
        System.out.println("ScholarAthlete studies");
    }

    public void train() {
        System.out.println("ScholarAthlete trains");
    }
}

// 5. Hybrid Inheritance (Combination via classes and interfaces)
interface Artist {
    void draw();
}

class AllRounder extends ScholarAthlete implements Student, Athlete, Artist {
    public void draw() {
        System.out.println("AllRounder draws");
    }
}

public class InheritanceDemo {
    public static void main(String[] args) {
        // Single Inheritance
        Dog dog = new Dog();
        dog.speak();
        dog.bark();

        // Multilevel Inheritance
        Pup pup = new Pup();
        pup.speak();
        pup.bark();
        pup.weep();

        // Hierarchical Inheritance
        Cat cat = new Cat();
        cat.speak();
        cat.meow();

        // Multiple Inheritance through interfaces
        ScholarAthlete sa = new ScholarAthlete();
        sa.study();
        sa.train();

        // Hybrid Inheritance
        AllRounder ar = new AllRounder();
        ar.study();
        ar.train();
        ar.draw();
    }
}