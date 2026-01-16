package tec.poo;

import java.lang.*;


public class Foo {

    public final static int MY_CONSTANT = 100;

    public static int MY_NUMBER = 7;

    private int myNumber;

    private Bar anotherBar;

    /**
     * Constructor
     */
    public Foo() {
        System.out.println("New Instance");

        this.myNumber = 8;
    }

    public Foo(int myNumber) {
        // hay que validar my number
        if (myNumber > 0 ) {
            this.myNumber = myNumber;
        }
    }

    public Foo(Bar bar) {

    }

    public static void myStaticMethod() {
        System.out.println("My Static method.");
    }

    public void someMethod() {
        System.out.println("Some method.");
        myStaticMethod();
    }

    public int getMyNumber() {
        return this.myNumber;
    }

    private void myHelper() {
        // pueda ser que aqui no se necesite validacion
        // porque este metodo psolamente puede ser invocado desde un metodo publico
        // y nosotros esperamos que es metodo public haya hecho la tarea de validar
    }

    private int number;

    public void setNumber(int newNumber) {  // 1
        this.number = newNumber;
    }
    public int multiply(int other) {   // 2
        return this.number * other;
    }
    public void someOtherMethod(Bar bar) {   //3
        if (bar != null) {
            bar.doSomething(); // verificar si bar es null
        } else {
            System.err.println("bar is null");
        }
    }
}