package ru.scarlet.authservice.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.scarlet.authservice.dto.RoleRequest;
import ru.scarlet.authservice.dto.RoleResponse;
import ru.scarlet.authservice.entity.Role;
import ru.scarlet.authservice.service.RoleService;

import java.util.List;

@RestController
@RequestMapping("/api/v1/roles")
@RequiredArgsConstructor
public class RoleController {
    private final RoleService roleService;

    @GetMapping("/")
    public List<RoleResponse> getRoles(){
        return roleService.getAll();
    }

    @PostMapping("/")
    public Role addRole(@RequestBody RoleRequest roleRequest){
        return roleService.addRole(roleRequest);
    }
}