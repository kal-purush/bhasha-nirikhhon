package com.grievance.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Custom exception class for handling if user is authorised
 * This exception is thrown when an attempt
 * when unauthorised user tries to access.
 */
@ResponseStatus(
  value = HttpStatus.CONFLICT,
  reason = "Unauthorised User"
)
public class UnauthorisedUserException extends RuntimeException {

   private static final long serialVersionUID = -3332292346834265674L;

/**
    * Constructs a new ResourceNotFoundException with a specific email address.
    *
    * @param email The email address for which the employee was not found.
    */
   public UnauthorisedUserException(final String email) {
      super("Unauthorized user=" + email);
   }
}