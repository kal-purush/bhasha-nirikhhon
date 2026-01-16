package com.example.ticketing.service;

import com.example.ticketing.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import com.example.ticketing.user.User;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
@Service
public class CustomUserDetailsService implements UserDetailsService {
    private final UserRepository userRepository;

    @Autowired
    public CustomUserDetailsService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        User user = userRepository.findByUsername(username);
        if (user == null) {
            throw new UsernameNotFoundException("User with name:" + username + "not found.");
        }
        String userRole= user.getUserRole().toString().toLowerCase();
        List<GrantedAuthority> authorities = new ArrayList<>();

        switch (userRole){
            case "admin":
                authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
                authorities.add(new SimpleGrantedAuthority("ROLE_EXTERNAL"));
                authorities.add(new SimpleGrantedAuthority("ROLE_INTERNAL"));
                break;
            case "internal":
                authorities.add(new SimpleGrantedAuthority("ROLE_EXTERNAL"));
                authorities.add(new SimpleGrantedAuthority("ROLE_INTERNAL"));
                break;
            default:
                authorities.add(new SimpleGrantedAuthority("ROLE_EXTERNAL"));
        }

        return new org.springframework.security.core.userdetails.User(
                user.getUsername(),
                user.getPassword(),
                authorities
        );


    }

}