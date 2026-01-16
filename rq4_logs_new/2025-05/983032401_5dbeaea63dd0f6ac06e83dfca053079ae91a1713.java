package com.api.NomNomCounter.adaptors.user;

import com.api.NomNomCounter.adaptors.out.user.persistence.UserEntity;
import com.api.NomNomCounter.adaptors.out.user.persistence.UserRepository;
import com.api.NomNomCounter.application.core.domain.user.User;
import com.api.NomNomCounter.application.ports.out.user.InsertUserOutPort;
import org.springframework.stereotype.Component;

@Component
public class InsertUserAdapter implements InsertUserOutPort {
    private final UserRepository userRepository;

    public InsertUserAdapter(UserRepository userRepository){
        this.userRepository = userRepository;
    }

    @Override
    public void save(User user) throws Exception {
        userRepository.save(UserEntity.builder()
                        .username(user.getUsername())
                        .passwordUser(user.getPasswordUser())
                        .build());
    }
}