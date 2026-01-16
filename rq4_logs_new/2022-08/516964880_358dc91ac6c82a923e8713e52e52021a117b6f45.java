package com.leeminjung1.domain.application.impl;

import com.leeminjung1.domain.application.UserService;
import com.leeminjung1.domain.application.dtos.RegisterDto;
import com.leeminjung1.domain.model.user.User;
import com.leeminjung1.infrastructure.repository.JpaUserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final JpaUserRepository userRepository;

    public User findById(long userId) {
        return userRepository.findById(userId). get();
    }

    public void register(RegisterDto dto) {
        ret
    }
}