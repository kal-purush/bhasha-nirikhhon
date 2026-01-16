package org.rentifytools.security;

import lombok.RequiredArgsConstructor;
import org.rentifytools.entity.User;
import org.rentifytools.exception.NotFoundException;
import org.rentifytools.service.UserService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CustomUserDetailsService implements UserDetailsService {

    private final UserService userService;

    @Override
    public UserDetails loadUserByUsername(String email) throws NotFoundException {

        Optional<User> user = userService.getUserByEmail(email);

        return user.map(CustomUserDetails::new).orElseThrow(() -> new NotFoundException(email));
    }
}