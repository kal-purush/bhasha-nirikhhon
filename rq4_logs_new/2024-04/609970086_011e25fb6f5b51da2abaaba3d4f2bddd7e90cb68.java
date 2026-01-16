package com.bearAndPupperCo.sangenWrestlingApp.Security.Service;

import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.Role;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.User;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.GrantedAuthority;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class UserDetailsTest {

    @InjectMocks
    UserDetailsImpl userDetails;

    @Test
    void buildUserDetailsTest() {
        //Arrange
        Role roleAdmin = new Role(1L, "ADMIN");
        Role roleUser = new Role(2L, "USER");
        Set<Role> roles = new HashSet<>(Arrays.asList(roleUser, roleAdmin));

        User user = new User("TestUser","testuser@email.com",
                "userPassword", LocalDate.now(), roles);

        //Act
        UserDetailsImpl returnedUser = userDetails.build(user);

        //Assert
        assertEquals(returnedUser.getUsername(), user.getUsername());
        assertEquals(returnedUser.getEmail(), user.getEmail());
        assertEquals(returnedUser.getPassword(), user.getPassword());

        Collection<? extends GrantedAuthority> authorities = returnedUser.getAuthorities();
        assertEquals(2, authorities.size());
        assertTrue(authorities.stream()
                .anyMatch(authority -> authority.getAuthority().equals("USER")));
        assertTrue(authorities.stream()
                .anyMatch(authority -> authority.getAuthority().equals("ADMIN")));

    }
}