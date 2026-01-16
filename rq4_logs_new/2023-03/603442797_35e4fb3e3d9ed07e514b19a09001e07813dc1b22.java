package homework.day10;

public class GenericMethodsInGenericClassT<T> {

    public void genericMethodOneGenArg(T incoming) {
        System.out.println("I am an object of " + incoming.getClass().getSimpleName() + " class");

    }

    public String genericMethodTwoGenArgs(T incomingA, T incomingB) {
        return ("We are objects of " + incomingA.getClass().getSimpleName() + " class and " + incomingB.getClass().getSimpleName() + " class");

    }

    public void genericMethodHalfGenArgs(T incomingC, String incomingD) {
        System.out.println("I got an object of " + incomingC.getClass().getSimpleName() + " class and string with " + incomingD.length() + " characters");

    }
}