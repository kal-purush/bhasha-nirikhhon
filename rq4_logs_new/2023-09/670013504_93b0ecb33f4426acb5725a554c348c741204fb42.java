package com.grievance.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * custom exception method to handle error when department not found.
 */
@ResponseStatus(value = HttpStatus.NOT_FOUND,
reason = "Department with this department Id doesn't exist")
public class DepartmentNotFound extends RuntimeException {
  private static final long serialVersionUID = -3332292346834265391L;

  /**
   * Constructs a new ResourceNotFoundException with a specific email address.
   */
  public DepartmentNotFound() {
    super("Department doesn't found with this Id");
  }
}