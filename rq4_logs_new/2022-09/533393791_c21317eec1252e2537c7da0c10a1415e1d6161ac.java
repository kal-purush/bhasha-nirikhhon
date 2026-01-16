package ua.lyashko.lesson6.Task1;

public class Main {
    public static void main ( String[] args ) {
        Phone phone1 = new Phone (  );
        phone1.model = "iPhone";
        phone1.number = 38095;
        phone1.weight = 1;

        Phone phone2 = new Phone (  );
        phone2.model = "Samsung";
        phone2.number = 38098;
        phone2.weight = 2;

        Phone phone3 = new Phone (  );
        phone3.model = "Xiaomi 520 yader";
        phone3.number = 38096;
        phone3.weight = 3;

        System.out.println ("model: " + phone1.model + " number: " + phone1.number + " weight: " + phone1.weight );
        System.out.println ("model: " + phone2.model + " number: " + phone2.number + " weight: " + phone2.weight );
        System.out.println ("model: " + phone3.model + " number: " + phone3.number + " weight: " + phone3.weight );

        phone1.receiveCall ();
        phone1.getNumber ();

        phone2.receiveCall ();
        phone2.getNumber ();

        phone3.receiveCall ();
        phone3.getNumber ();
    }
}