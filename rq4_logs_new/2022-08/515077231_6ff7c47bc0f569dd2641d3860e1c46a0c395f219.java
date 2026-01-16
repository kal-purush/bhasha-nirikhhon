package com.codecool.timebuyers.auth;

import com.codecool.timebuyers.dao.UserStorageRepository;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
public class ApplicationUserService implements UserDetailsService {

    private final UserStorageRepository userStorageRepository;

    public ApplicationUserService(UserStorageRepository userStorageRepository) {
        this.userStorageRepository = userStorageRepository;
    }


    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return userStorageRepository
                .findByUserName(username)
                .orElseThrow(() ->
                        new UsernameNotFoundException(String.format("Username %s not found", username))
                );
    }
}