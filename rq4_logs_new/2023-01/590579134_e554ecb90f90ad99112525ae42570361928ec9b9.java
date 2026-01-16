package vask.pet.swapme.userservice.exeption;

import org.springframework.http.HttpStatus;

public class KeycloakBasicException extends CustomErrorException{
    public KeycloakBasicException() {
    }

    public KeycloakBasicException(String message) {
        super(message);
    }

    public KeycloakBasicException(HttpStatus status, String message) {
        super(status, message);
    }

    public KeycloakBasicException(HttpStatus status, String message, Object data) {
        super(status, message, data);
    }
}