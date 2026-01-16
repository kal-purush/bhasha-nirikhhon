package com.puggysoft.controllers.salary;

import com.puggysoft.services.salary.ServiceSalaryEmployeeDeleteById;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerSalaryEmployeeDeleteById {

  @Autowired
  private ServiceSalaryEmployeeDeleteById serviceSalaryEmployeeDeleteById;

  @DeleteMapping(path = "/api/v1/salary-employee/{id}")
  public ResponseEntity<String> deleteById(@PathVariable Long id) {
    return serviceSalaryEmployeeDeleteById.deleteById(id);
  }
}