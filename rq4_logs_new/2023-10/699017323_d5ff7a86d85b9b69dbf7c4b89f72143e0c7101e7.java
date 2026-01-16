package core.exceptions;

public class CarFuelException extends Exception{
    public CarFuelException(){

    }

    public CarFuelException(String message){
        super(message);
    }

    public CarFuelException(String message, Throwable cause){
        super(message, cause);
    }

    public CarFuelException(Throwable cause){
        super(cause);
    }

    public CarFuelException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace){
        super(message, cause, enableSuppression, writableStackTrace);
    }
}