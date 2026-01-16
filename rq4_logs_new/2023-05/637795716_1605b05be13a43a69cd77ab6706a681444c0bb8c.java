package com.exadel.carinsurance;

import com.exadel.carinsurance.exceptions.AlreadyExistsException;
import com.exadel.carinsurance.exceptions.NotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
  @ExceptionHandler( NotFoundException.class )
  public ResponseEntity notFoundException( NotFoundException ex ) {
    return ResponseEntity
        .status( HttpStatus.FORBIDDEN )
        .body( ex.getMessage() );
  }

  @ExceptionHandler( AlreadyExistsException.class )
  public ResponseEntity alreadyExistsException( AlreadyExistsException ex ) {
    return ResponseEntity
        .status( HttpStatus.CONFLICT )
        .body( ex.getMessage() );
  }

  @ExceptionHandler( UsernameNotFoundException.class )
  public ResponseEntity usernameNotFoundException( UsernameNotFoundException ex ) {
    return ResponseEntity
        .status( HttpStatus.NOT_FOUND )
        .body( ex.getMessage() );
  }

  @ExceptionHandler( BadCredentialsException.class )
  public ResponseEntity badCredentialsException( BadCredentialsException ex ) {
    return ResponseEntity
        .status( HttpStatus.NOT_FOUND )
        .body( "The entered password is incorrect" );
  }

  @ExceptionHandler( AuthenticationException.class )
  public ResponseEntity authenticationException( AuthenticationException ex ) {
    return ResponseEntity
        .status( HttpStatus.UNAUTHORIZED )
        .body( String.format( "Authentication error: %s", ex.getMessage() ) );
  }
}