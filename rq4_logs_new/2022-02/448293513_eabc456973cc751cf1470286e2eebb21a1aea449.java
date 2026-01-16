package HW6;

class Animal {
    // здесь задаем одинаковые типы данных для всех животных

    public String name;
    public int run;

    public Animal() {
    }

    public Animal(String name) {
        this.name = name;
    }

    public void animalInfo() {
        System.out.println();

    }
}

//**********************CAT****************************
//это подкласс Cat, супер класса Animal с наследованием extends
class Cat extends Animal {

    protected boolean sweem;


//konstruktor Cat

    public Cat(String name, int run, boolean sweem) {
        this.name = name;
        this.run = run;
        this.sweem = sweem;

    }

    //выводим информацию в консоль
    public void catInfo() {
        System.out.println("CatName: " + name + " /Пробежала: " + run+ " meters/" + " /Проплыла:" + sweem);
    }
}

//***********************DOG****************************
//это подкласс Cat супер класса Animal с наследованием extends
class Dog extends Animal {


    public int sweem;

    //konstruktor

    public Dog(String name, int run, int sweem) {
        this.name = name;
        this.run = run;
        this.sweem = sweem;

    }
    public void dogInfo() {
        System.out.println("DogName: " + name  + " /Пробежал: " + run+ " meters/" + " /Проплыл:" + sweem);
    }
}

 class Main {
    public static void main(String[] args) {

        Animal animal = new Animal("CatsCategory");
        Cat cat = new Cat("Жужа", 200, false);
        animal.animalInfo();
        cat.animalInfo();
        cat.catInfo();

        Animal danimal = new Animal("Dogs");
        Dog dog = new Dog("Дружок", 500, 10);
        danimal.animalInfo();
        dog.animalInfo();
        dog.dogInfo();

    }
}