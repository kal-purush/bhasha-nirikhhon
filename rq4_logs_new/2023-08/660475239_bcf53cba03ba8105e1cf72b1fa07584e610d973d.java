package day22overridingEncapsulation;

public class Cat extends Mammal{

    @Override  //Override Annotation ==> checks the overriding rules
    public void eat() {
        System.out.println("Cats eat...");
    }

    @Override
    public Mammal opccur() {
        return new Cat();
    }

    @Override
    public Integer number() {
        return 456;
    }

    @Override
    public int num() {
        return 24;
    }

    @Override
    public Cat create() {
        return new Cat();





    }
}