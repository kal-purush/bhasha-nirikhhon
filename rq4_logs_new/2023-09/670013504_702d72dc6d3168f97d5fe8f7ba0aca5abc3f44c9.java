/**
 *
 */
package com.grievance.authentication;

import com.grievance.entity.Employee;
import com.grievance.entity.UserType;
import com.grievance.exception.ResourceNotFoundException;
import com.grievance.repository.EmployeeRepository;

import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 */
public class AuthenticatingUserImpl implements AuthenticatingUser {
  /**
   * employeeRepository instance for
   * access to database.
   */
  @Autowired
  private EmployeeRepository employeeRepository;

  /**
   * method to check if user exists or not.
   * @param email
   * @param password
   * @return boolean
   */
  @Override
  public Boolean checkIfUserExists(final String email, final String password) {
      Employee employee = employeeRepository.findByEmail(email);
      if (employee == null) {
         throw new ResourceNotFoundException(email);
      }
    return employee.getPassword().equals(password);
  }

  /**
   * method to check if user is authorised or not.
   * @param email
   * @param password
   * @return boolean
   */
  @Override
  public Boolean checkIfUserIsAdmin(
        final String email, final String password) {
        Employee employee = employeeRepository.findByEmail(email);
      if (employee == null) {
         throw new ResourceNotFoundException(email);
      }
      return employee.getPassword().equals(password)
         && employee.getUserType().equals(UserType.ADMIN);
  }
}