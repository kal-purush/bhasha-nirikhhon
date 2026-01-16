package homework.day10;

public class GenericMethodsInGenericClassTwoParams<X, Y> {
    public String genericMethodGenArgs(X incoming) {
        return new String("I received 1 argument of type: " + incoming.getClass().getSimpleName() + " class");
    }

    public String genericMethodGenArgs(X incomingA, Y incomingB) {
        return new String("I received 2 arguments of type: " + incomingA.getClass().getSimpleName()
                + " class, " + incomingB.getClass().getSimpleName() + " class");
    }

    public void genericMethodHalfGenArgs(X incomingC, String incomingD) {
        System.out.printf("I got an object of %s class and string with %s characters",
                incomingC.getClass().getSimpleName(), incomingD.length()).println();
    }

    public void genericMethodHalfGenArgs(X incomingE, Y incomingF, String incomingG) {
        System.out.printf("I got an object of %s class and %s class and string with %s characters",
                incomingE.getClass().getSimpleName(), incomingF.getClass().getSimpleName(), incomingG.length()).println();
    }

}