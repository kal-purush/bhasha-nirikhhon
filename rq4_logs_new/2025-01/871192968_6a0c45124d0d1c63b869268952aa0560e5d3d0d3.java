package santiagobarquero.airportsys.application.exceptions;

public final class ExceptionMessagesHelper {

    private ExceptionMessagesHelper() {
        super();
    }

    public static String getEntityNotFoundExceptionMsg(String entityName) {
        return String.format("Entity '%s' does not exists", entityName);
    }
}