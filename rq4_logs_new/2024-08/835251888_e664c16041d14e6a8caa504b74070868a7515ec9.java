package dev.forkingaround.zootopia.services;

import dev.forkingaround.zootopia.models.SecurityUser;
import dev.forkingaround.zootopia.models.User;
import dev.forkingaround.zootopia.repositories.UserRepository;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class JpaUserDetailsServiceTest {

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private JpaUserDetailsService jpaUserDetailsService;

    JpaUserDetailsServiceTest() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testLoadUserByUsername_UserFound() {
        User user = new User();
        user.setUsername("john_doe");
        user.setPassword("password123");

        when(userRepository.findByUsername("john_doe")).thenReturn(Optional.of(user));

        UserDetails userDetails = jpaUserDetailsService.loadUserByUsername("john_doe");

        assertNotNull(userDetails);
        assertTrue(userDetails instanceof SecurityUser);
        assertEquals("john_doe", userDetails.getUsername());
        assertEquals("password123", userDetails.getPassword());
        verify(userRepository).findByUsername("john_doe");
    }

    @Test
    void testLoadUserByUsername_UserNotFound() {
        when(userRepository.findByUsername(anyString())).thenReturn(Optional.empty());

        UsernameNotFoundException thrown = assertThrows(
            UsernameNotFoundException.class,
            () -> jpaUserDetailsService.loadUserByUsername("non_existent_user")
        );

        assertEquals("User not found non_existent_user", thrown.getMessage());
        verify(userRepository).findByUsername("non_existent_user");
    }
}