package vask.pet.swapme.userservice.controller.handler.exeption;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import vask.pet.swapme.userservice.exeption.CustomErrorException;
import vask.pet.swapme.userservice.exeption.util.ErrorResponse;

import java.io.PrintWriter;
import java.io.StringWriter;


@ControllerAdvice
class CustomControllerAdvice {
    // ...
    /**
     * handleException - Handles all the CustomErrorExceptions recieving an Exception, ErrorResponse.
     *
     * @param e
     * @return ResponseEntity<ErrorResponse>
     * @user vask19
     * @since 20
     */
    @ExceptionHandler(CustomErrorException.class)
    public ResponseEntity<ErrorResponse> handleCustomErrorExceptions(Exception e) {
        CustomErrorException customErrorException = (CustomErrorException) e;
        HttpStatus status = customErrorException.getStatus();
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        customErrorException.printStackTrace(printWriter);
        String stackTrace = stringWriter.toString();

        return new ResponseEntity<>(
                new ErrorResponse(
                        status,
                        customErrorException.getMessage(),
                        stackTrace,
                        customErrorException.getData()
                ),
                status
        );
    }

}