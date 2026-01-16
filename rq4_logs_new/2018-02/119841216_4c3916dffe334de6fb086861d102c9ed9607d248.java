package skadic.commands;

public class CommandNotFoundException extends RuntimeException {

    private String name;
    private static final String msg = "Command not found with name: ";

    public CommandNotFoundException(String name) {
        super(msg + name);
        this.name = name;
    }

    public CommandNotFoundException(Throwable cause, String name) {
        super(msg + name, cause);
        this.name = name;
    }

    public CommandNotFoundException(Throwable cause, boolean enableSuppression, boolean writableStackTrace, String name) {
        super(msg + name, cause, enableSuppression, writableStackTrace);
        this.name = name;
    }

    public String getName() {
        return name;
    }
}