package manager.model.exceptions;

public class IncorrectUsernameOrPasswordException extends Exception{
    public IncorrectUsernameOrPasswordException(String errorMessage){
        super(errorMessage);
    }
}