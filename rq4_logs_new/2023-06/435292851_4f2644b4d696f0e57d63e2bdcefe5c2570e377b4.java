package com.puggysoft.controllers.alcaldia;

import com.puggysoft.dtos.alcaldia.DtoAlcaldiaRecursosMunicipales;
import com.puggysoft.dtos.alcaldia.DtoAlcaldiaRecursosMunicipalesFilter;
import com.puggysoft.services.alcaldia.ServiceAlcaldiaRecursosMunicipalesGetFilterByVentasId;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerAlcaldiaRecursosMunicipalesGetFilterByVentasId {

  @Autowired
  private ServiceAlcaldiaRecursosMunicipalesGetFilterByVentasId serviceAlcaldiaRecursosMunicipalesGetFilter;

  // products/filter?page=${activePage - 1}&size=${pageSize}&saleId=${saleId}
  @PostMapping(path = "/api/v1/alcaldia/filter-by-ventas-id/filter")
  public ResponseEntity<List<DtoAlcaldiaRecursosMunicipales>> getAlcaldiaRecursosMunicipales(
      @RequestParam int page,
      @RequestParam int size,
      @RequestParam Long ventasId,
      @RequestBody @Valid DtoAlcaldiaRecursosMunicipalesFilter dtoAlcaldiaRecursosMunicipales) {
    return serviceAlcaldiaRecursosMunicipalesGetFilter.filter(ventasId, dtoAlcaldiaRecursosMunicipales, page, size);
  }
}