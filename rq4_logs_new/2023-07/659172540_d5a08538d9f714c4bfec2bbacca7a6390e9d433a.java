package stepikCourse.Java.interfacesAndAbstract;

public class InterfaceTest1 {
    public static void main(String[] args) {
        Teacher t = new Teacher();
        t.pomosh();
        t.tushitPojar();
        Driver d = new Driver();
        d.pomosh();
        d.swim();
        d.tushitPojar();
    }

}
class Employee {
    double salary = 100;
    String name = "Nick";
    int age;
    int experience;
    void eat() {
        System.out.println("kushat");
    }
    void sleep() {
        System.out.println("Spat");
    }
}
  class Teacher extends Employee implements Help_able {
    int kolichestvoUchenikov;
    void uchit() {
        System.out.println("Uchit");
    }
     public void pomosh() {
        System.out.println("Uchitel okazivaet pomosh");
    }
    public void tushitPojar() {
        System.out.println("Uchitel tushit pojar");
    }
}

/**
 * в классе Driver происходит множественная имплементация интерфейсов но не затронуто extends
 */
 class Driver extends Employee implements Help_able, Swim_able {
    String nazvanieMashini;
    void vodit() {
        System.out.println("Vodit");
    }

     @Override
     public void pomosh() {
         System.out.println("Driver okazuvaet pomosh");
     }

     @Override
     public void tushitPojar() {
         System.out.println("Driver tushit pojar");

     }

     @Override
     public void swim() {
         System.out.println("Driver plavaet");
     }
 }

 interface Help_able{
    void pomosh();
    void tushitPojar();
 }

 interface Swim_able{
    void swim();
 }
