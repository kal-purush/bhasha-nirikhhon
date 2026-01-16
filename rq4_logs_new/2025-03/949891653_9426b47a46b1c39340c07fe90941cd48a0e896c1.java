package com.chapssal_tteok.preview.domain.user.converter;

import com.chapssal_tteok.preview.domain.user.dto.UserResponseDTO;
import com.chapssal_tteok.preview.domain.user.entity.User;

public class UserConverter {

    public static UserResponseDTO.UserDTO toUserDTO(User user) {
        return UserResponseDTO.UserDTO.builder()
                .username(user.getUsername())
                .name(user.getName())
                .email(user.getEmail())
                .build();
    }

    public static UserResponseDTO.UserInfoDTO toUserInfoDTO(User user) {
        return UserResponseDTO.UserInfoDTO.builder()
                .role(user.getRole())
                .username(user.getUsername())
                .name(user.getName())
                .email(user.getEmail())
                .build();
    }
}