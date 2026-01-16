package uj.pwkp.gr1.vet.VetApp.controller.rest.response;

import lombok.Value;
import uj.pwkp.gr1.vet.VetApp.entity.User;

@Value
public class UserResponseDto {
    String username;
    String role;

    public UserResponseDto(User user) {
        this.username = user.getUsername();
        this.role = user.getRole().name();
    }
}