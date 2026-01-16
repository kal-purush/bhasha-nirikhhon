package com.example.registerstartupproject.adminPanel;

import com.example.registerstartupproject.Repository.Entity.RegisterTeam;
import com.example.registerstartupproject.Repository.Entity.Role;
import com.example.registerstartupproject.Repository.RegisterTeamRepository;
import com.example.registerstartupproject.Repository.RoleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@RequiredArgsConstructor
public class AdminSeed {
    @Value("${admin.password}")
    private String password;
    @Value("${admin.email}")
    private String email;

    private final RoleRepository roleRepository;
    private final RegisterTeamRepository registerTeamRepository;

    public void createAdminAccount() {
        if (!registerTeamRepository.existsByEmail(email)) {
            Role role = new Role("ADMIN");
            if (!roleRepository.existsById("ADMIN")) {
                roleRepository.save(role);
            }
            RegisterTeam registerTeam = new RegisterTeam();
            registerTeam.setPassword(password);
            registerTeam.setEnabled(true);
            registerTeam.setRoles(Set.of(role));
            registerTeam.setUsername("Admin");
            registerTeam.setEmail(email);
            registerTeamRepository.save(registerTeam);
        }

    }
}