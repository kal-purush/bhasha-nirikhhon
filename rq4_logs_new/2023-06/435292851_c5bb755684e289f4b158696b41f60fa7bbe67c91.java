package com.puggysoft.controllers.alcaldia;

import com.puggysoft.dtos.alcaldia.DtoUrbanismoRequisitos;
import com.puggysoft.dtos.alcaldia.DtoUrbanismoRequisitosFilter;
import com.puggysoft.services.alcaldia.ServiceRequisitosGetFilterWithTramites;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerRequisitosGetFilterWithTramites {

  @Autowired
  private ServiceRequisitosGetFilterWithTramites serviceRequisitosGetFilterWithTramites;

  @PostMapping(path = "/api/v1/requisitos/filter/with-tramites")
  public ResponseEntity<List<DtoUrbanismoRequisitos>> getRequisitos(
      @RequestParam int page,
      @RequestParam int size,
      @RequestBody @Valid DtoUrbanismoRequisitosFilter dtoUrbanismoRequisitosFilter,
      @RequestParam String tramite) {
    return serviceRequisitosGetFilterWithTramites.filter(dtoUrbanismoRequisitosFilter, page, size, tramite);
  }
}