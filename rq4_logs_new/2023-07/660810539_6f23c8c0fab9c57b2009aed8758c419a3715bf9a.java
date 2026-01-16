package studentenrollment.Academicinfo.exception;

import java.util.ArrayList;
import java.util.List;

public class AcademyServiceException extends RuntimeException{
    private final List<String> errors;


    public AcademyServiceException(String message, List<String> errors) {
        super(message);
        this.errors = errors;
    }

    public List<String> getErrors() {
        return errors;
    }
}