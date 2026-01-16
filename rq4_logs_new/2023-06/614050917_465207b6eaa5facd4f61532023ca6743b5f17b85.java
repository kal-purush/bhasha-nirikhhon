package spring.summer.socialnetwork.exceptions;

public class TooWeakPassword extends Exception{

    public TooWeakPassword() {
        super("Too weak password");
    }
}