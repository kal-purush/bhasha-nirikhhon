package com.soteria.controllers;

import com.soteria.models.Role;
import com.soteria.services.RoleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(path = "api/v1/role")
public class RoleController {
    private final RoleService roleService;

    @Autowired
    public RoleController(RoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping(path = "/all")
    public List<Role> getRoles() {
        return roleService.getRoles();
    }

    @GetMapping(path = "/{id}")
    public Role getRole(@PathVariable("id") Long id) {
        return roleService.getRole(id);
    }

    @PostMapping(path = "/add")
    public Role addRole(@RequestBody Role role) {
        return roleService.addRole(role);
    }

    @PutMapping(path = "/update/{id}")
    public Role updateRole(@PathVariable("id") Long id,
                           @RequestParam("name") String name) {
        return roleService.updateRole(id, name);
    }

    @DeleteMapping("/delete/{id}")
    public void deleteRole(@PathVariable("id") Long id) {
        roleService.deleteRole(id);
    }
}