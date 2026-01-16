package main.java.com.testapp.afor.pattern;

public class AnimalFactory {

    public Animal getAnimal(String animalName) {

        if (animalName == null) {
            return null;
        }

        if (animalName.equalsIgnoreCase("cow")) {
            return new Cow();

        }else if (animalName.equalsIgnoreCase("mouse")){
            return new Mouse();

        }else if (animalName.equalsIgnoreCase("dog")){
            return new Dog();

        }
        return null;
    }
}