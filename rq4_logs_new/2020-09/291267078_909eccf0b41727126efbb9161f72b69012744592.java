package hangman.model;

public class modelException extends Exception {

    /**
     * Creates a new instance of <code>modelException</code> without detail
     * message.
     */
    public modelException() {
    }

    /**
     * Constructs an instance of <code>modelException</code> with the specified
     * detail message.
     *
     * @param msg the detail message.
     */
    public modelException(String msg) {
        super(msg);
    }
} 