package com.puggysoft.controllers.system;

import com.puggysoft.services.system.ServiceTenantDeleteById;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerTenantDeleteById {

  @Autowired
  private ServiceTenantDeleteById serviceTenantDeleteById;

  @DeleteMapping(path = "/api/v1/tenants/{id}")
  public ResponseEntity<String> deleteById(@PathVariable Long id) {
    return serviceTenantDeleteById.deleteById(id);
  }
}