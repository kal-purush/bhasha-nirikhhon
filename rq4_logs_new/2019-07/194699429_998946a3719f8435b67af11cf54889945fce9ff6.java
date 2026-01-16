package Models;

public class ErrorResponse extends Response{

    public ErrorResponse(String message) {
        super(message);
        setType(PackageType.ERROR);
    }
}