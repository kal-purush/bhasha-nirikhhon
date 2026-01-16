package br.com.taggedalbum.exception;

/**
 * Created by rafaelperetta on 8/23/16.
 */
public class UserNotFoundException extends ResourceNotFoundException {

    public UserNotFoundException(String message, Long id) {
        super(message, id);
    }
}