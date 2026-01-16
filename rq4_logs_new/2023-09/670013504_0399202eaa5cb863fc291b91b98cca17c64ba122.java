package com.grievance.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Custom exception class for handling  not found errors.
 * This exception is thrown when an attempt
 * to retrieve an employee by email fails.
 */
@ResponseStatus(
  value = HttpStatus.NOT_FOUND,
  reason = "Ticket with this ticket Id doesn't exist"
) // HTTP 404
public class TicketNotFoundException extends RuntimeException {
  private static final long serialVersionUID = -3332292346834265371L;

  /**
   * Constructs a new ResourceNotFoundException with a specific email address.
   *
   * @param ticketId The ticketId for which the Ticket was not found.
   */
  public TicketNotFoundException(final Integer ticketId) {
    super("Ticket doesn't exist with this ticketId " + ticketId);
  }
}