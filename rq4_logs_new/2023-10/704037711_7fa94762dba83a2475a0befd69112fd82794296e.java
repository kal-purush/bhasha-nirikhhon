package lld2_oop2_access_modifiers_and_constructors.encapsulation_protection_access_modifier;

public class Student {

    private String name; // am: private
    int age; // am: default
    protected  String gender; // am: protected
    public String batch; // am: public

    private void do1 () { // am: private
        System.out.println("Doing 1");
    }

    void do2() { // am: default
        System.out.println("Doing 2");
    }

    protected void do3 () {
        System.out.println("Doing 3");
    }

    public void do4 () {
        System.out.println("Doing 4");
    }
}