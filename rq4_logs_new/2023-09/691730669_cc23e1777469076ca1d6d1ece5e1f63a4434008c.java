package md.tkwillacademy.classesandoobjects;

import md.tkwillacademy.classesandoobjects.autoservicetask.Car;
import md.tkwillacademy.classesandoobjects.autoservicetask.Garage;
import md.tkwillacademy.classesandoobjects.autoservicetask.Person;
import md.tkwillacademy.classesandoobjects.autoservicetask.Worker;

import java.util.concurrent.ThreadPoolExecutor;

public class ManageAutoService {
    public static void main(String[] args) {

        Garage autoDocGarage;
        autoDocGarage = new Garage();
        autoDocGarage.address = "Stefan cel Mare, Ihub 64";
        autoDocGarage.surfaceM2 = 450.0F;
        autoDocGarage.capacity = 50;
        System.out.println( "Obiectul autoDocGarage are urmatoarele proprietati " + autoDocGarage.address + " "
        + autoDocGarage.surfaceM2 + "  " + autoDocGarage.capacity);

        Garage garajIaloveni;
        garajIaloveni = new Garage();
        System.out.println( "Obiectul garajIaloveni are urmatoarele proprietati " + garajIaloveni.address + " "
                +  garajIaloveni.surfaceM2 + "  " + garajIaloveni.capacity);

        Worker vasile = new Worker();
        vasile.name = "vasile";
        vasile.age = 54;
        System.out.println("Obiectul nostru are numele de " + vasile.name + " si are " + vasile.age + " ani");

        Worker ion = new Worker();
        System.out.println("Obiectul nostru are numele de " + ion.name + " si are " + ion.age + " ani");


        Person client1 = new Person("+37360122115");
        Car porscheNKK122 = new Car("AAAswwd12515151", "Porsche Cayene",client1);
        Car mercedesBenzTransit = new Car("ADSDS4554454","Mercedes Benz Transit",
                new Person("+37384848488"));
        System.out.println("Masina noastra are numele de " + porscheNKK122.mark +
                " si proprietarul poate fi apelat la " + porscheNKK122.owner.phoneNumber);
        System.out.println("Masina noastra are numele de " + mercedesBenzTransit.mark +
                " si proprietarul poate fi apelat la " + mercedesBenzTransit.owner.phoneNumber);
    }
}