package com.puggysoft.controllers.alcaldia;

import com.puggysoft.services.alcaldia.ServiceUrbanismoEstadosDeleteById;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerUrbanismoEstadosDeleteById {

  @Autowired
  private ServiceUrbanismoEstadosDeleteById serviceUrbanismoEstadosDeleteById;

  @DeleteMapping(path = "/api/v1/urbanismo-estados/{id}")
  public ResponseEntity<String> deleteById(@PathVariable Long id) {
    return serviceUrbanismoEstadosDeleteById.deleteById(id);
  }
}