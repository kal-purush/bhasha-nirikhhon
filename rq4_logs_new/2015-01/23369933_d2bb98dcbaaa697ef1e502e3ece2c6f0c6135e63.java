package chapter9.exercise03;

// See: http://docs.oracle.com/javase/8/docs/technotes/guides/language/catch-multiple.html
public class MultipleExceptionTypes {

    public void process(String exceptionName)
            throws FirstException, SecondException {
        try {
            if (exceptionName.equals("First")) {
                throw new FirstException();
            } else {
                throw new SecondException();
            }
        } catch (FirstException | SecondException ex) {
            throw ex;
        }
    }
}

class FirstException extends Exception {
    private static final long serialVersionUID = -5633417755399990481L;
}

class SecondException extends Exception {
    private static final long serialVersionUID = -8927089808076716219L;
}