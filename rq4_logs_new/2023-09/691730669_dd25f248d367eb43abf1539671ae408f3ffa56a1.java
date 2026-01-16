package md.accessmodifiers;

import md.accessmodifiers.peopleevidence.Person;

import java.sql.SQLOutput;

public class ChisinauEvidenceCatalog {
    public static void main(String[] args) {

        Person cristina = new Person();
        System.out.println(cristina.isRetired);
        cristina.isRetired = true;

        if (cristina.isRetired) {
            System.out.println("Cristina is retired");
        } else {
            System.out.println("Cristina is not retired");
        }
        Person eliza = new Person(545454554, "Eliza", "Maried", true);
        System.out.println("Available info about ELiza: " + eliza.isRetired);

        if (eliza.isRetired) {
            System.out.println("Eliza is retired");
        } else {
            System.out.println("Eliza is not retired");

        }

        Person marcel = new Person(564654654, "Marcel");
        System.out.println("Available info about Marcel: " + marcel.isRetired);

        if (marcel.isRetired) {
            System.out.println("Marcel is retired");
        } else {
            System.out.println("Marcel is not retired");
        }

        System.out.println(Person.hasHeart);
        Person.hasBrain = true;
        System.out.println(Person.hasBrain);


    }

}