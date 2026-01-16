package com.cozy.domain.user.dao;

import com.cozy.domain.user.dto.CreateUserDto;
import com.cozy.domain.user.dto.GetOneUserDto;

public interface UserDao {
    Long createUser(CreateUserDto createUserDto);

    GetOneUserDto getOneUser(Long id);
}