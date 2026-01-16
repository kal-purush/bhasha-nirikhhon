package com.dalevents.vensy.services.auth;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.dalevents.vensy.common.AppError;
import com.dalevents.vensy.models.AppUser;
import com.dalevents.vensy.models.enums.Role;
import com.dalevents.vensy.repositories.AppUserRepository;
import com.dalevents.vensy.services.auth.request.RegisterCompanyUserCommand;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class AuthServiceImpl implements AuthService{
    private final AppUserRepository repository;

    private final PasswordEncoder passwordEncoder;
    
    @Override
    public void registerCompanyUser(RegisterCompanyUserCommand command) {
        var existingUser = repository.existsByEmail(command.email());

        if(existingUser){
            throw new AppError(HttpStatus.CONFLICT.value(), "Account already exists");
        }

        String hashPassword =  passwordEncoder.encode(command.password());
        AppUser newAppUser = AppUser.builder().email(command.email()).firstname(command.firstname()).lastname(command.lastname()).password(hashPassword).role(Role.COMPANY).build();
        
        repository.save(newAppUser);
    }
    
}