package com.example.backend;

import com.example.backend.dtos.UserDetailsDTO;
import com.example.backend.models.UserEntity;

public class TestHelper {
    public static UserEntity createUserEntity(UserDetailsDTO newUser) {
        UserEntity userEntity = new UserEntity();
        userEntity.setUsername(newUser.getUsername());
        userEntity.setPassword(newUser.getPassword());
        userEntity.setEnabled(newUser.isEnabled());
        userEntity.setDeleted(newUser.isDeleted());
        return userEntity;
    }
    public static UserEntity createUserEntity() {
        UserDetailsDTO newUser = createMockUserDto();
        UserEntity userEntity = new UserEntity();
        userEntity.setId(1L);
        userEntity.setUsername(newUser.getUsername());
        userEntity.setPassword(newUser.getPassword());
        userEntity.setEnabled(newUser.isEnabled());
        userEntity.setDeleted(newUser.isDeleted());
        return userEntity;
    }

    public static UserDetailsDTO createMockUserDto() {
        UserDetailsDTO newUser = new UserDetailsDTO();
        newUser.setUsername("testuser@user.at");
        newUser.setPassword("user1234");
        newUser.setEnabled(true);
        newUser.setDeleted(false);
        return newUser;
    }
}