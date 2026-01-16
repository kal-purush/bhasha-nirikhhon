package com.bearAndPupperCo.sangenWrestlingApp.Security.Service;

import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.Role;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Repository.RoleRepo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RoleServiceTest {

    @InjectMocks
    RoleServiceImpl roleService;

    @Mock
    RoleRepo roleRepo;

    @Test
    void addRolesToUserTest() {
        //Assert
        Set<String> rolesStr = new HashSet<>();
        rolesStr.addAll(Arrays.asList("ADMIN", "USER"));

        Optional<Role> roleAdmin = Optional.of(new Role(1L, "ADMIN"));
        Optional<Role> roleUser = Optional.of(new Role(2L, "USER"));

        when(roleRepo.findByName("ADMIN")).thenReturn(roleAdmin);
        when(roleRepo.findByName("USER")).thenReturn(roleUser);

        //Act
        Set<Role> rolesReturned = roleService.addRolesToUser(rolesStr);

        //Assert
        assertEquals(2, rolesReturned.size());
        assertEquals(roleAdmin, rolesReturned.stream().filter(role -> role.getName().equals("ADMIN")).findFirst());
        assertEquals(roleUser, rolesReturned.stream().filter(role -> role.getName().equals("USER")).findFirst());
    }

    @Test
    void cantFindRole() {
        //Assert
        Set<String> rolesStr = new HashSet<>();
        rolesStr.add("WRONG_ROLE");

        when(roleRepo.findByName("WRONG_ROLE")).thenReturn(Optional.empty());

        //Act/Assert
        assertThrows(RuntimeException.class, () -> roleService.addRolesToUser(rolesStr));
        verify(roleRepo, times(1)).findByName("WRONG_ROLE");
    }
}