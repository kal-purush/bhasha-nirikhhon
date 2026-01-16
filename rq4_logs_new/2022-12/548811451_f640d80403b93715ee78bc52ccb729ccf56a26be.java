package com.kienast.concurrency;

public class Deadlock2Main {

    public static void main(String[] args) {
        PolitePerson jane = new PolitePerson("Jane");
        PolitePerson john = new PolitePerson("John");

        new Thread(new Runnable() {
            @Override
            public void run() {
                jane.sayHello(john);
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                john.sayHello(jane);
            }
        }).start();

    }

    private static class PolitePerson {
        private final String name;

        private PolitePerson(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public synchronized void sayHello(PolitePerson person) {
            System.out.println(this.name + ": " + person.getName() + " has said hello to me! \n");
            sayHelloBack(person);
        }

        public synchronized void sayHelloBack(PolitePerson person) {
            System.out.println(person.getName() + ": " + this.name + " has said hello back to me! \n");
        }
    }

}