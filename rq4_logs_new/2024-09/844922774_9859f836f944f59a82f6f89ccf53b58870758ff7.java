package com.codecool.solarwatch.model.dto;

import com.codecool.solarwatch.model.user.Role;

public record UserDTO(String username, Role role) {
}