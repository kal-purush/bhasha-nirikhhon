package com.banking.bankservice.util;

import com.banking.bankservice.model.Role;
import com.banking.bankservice.model.User;
import com.banking.bankservice.service.RoleService;
import com.banking.bankservice.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

@Component
public class DataInitializer {
    private final UserService userService;
    private final RoleService roleService;

    @Autowired
    public DataInitializer(UserService userService, RoleService roleService) {
        this.userService = userService;
        this.roleService = roleService;
    }

    @PostConstruct
    public void init() {
        User admin = new User();
        admin.setName("admin");
        admin.setPhoneNumber("0661237698");
        admin.setPassword("admin");
        admin.setDateOfBirth(LocalDate.of(1989, 4, 25));
        Set<Role> roles = new HashSet<>();
        roles.add(roleService.getByRoleName("ADMIN"));
        admin.setRoles(roles);
        userService.save(admin);
    }
}