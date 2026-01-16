package com.puggysoft.controllers.users;

import com.puggysoft.dtos.users.DtoRole;
import com.puggysoft.dtos.users.DtoRoleFilter;
import com.puggysoft.services.users.ServiceRolesGetFilterWithUsersAndTenant;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ControllerRoleGetFilterWithUsersAndTenant {

  @Autowired
  private ServiceRolesGetFilterWithUsersAndTenant service;

  @PostMapping(path = "/api/v1/roles/filter/with-users-and-tenants")
  public ResponseEntity<List<DtoRole>> getUsers(
      @RequestParam int page,
      @RequestParam int size,
      @RequestBody @Valid DtoRoleFilter dtoFilter,
      @RequestParam Long idUser,
      @RequestParam String tenant) {
    return service.filter(dtoFilter, page, size, idUser, tenant);
  }
}