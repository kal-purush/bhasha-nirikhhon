package com.ichurch.backend.dto.User;

import com.ichurch.backend.model.User;
import lombok.*;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserViewDTO {

    private String email;
    private String firstName;
    private String lastName;
    private String cellphone;
    private String address;
    private Timestamp birthdate;
    private boolean visitor;

    public static UserViewDTO modelToDto(User user) {
        return UserViewDTO.builder()
                .email(user.getUsername())
                .firstName(user.getFirstName())
                .lastName(user.getLastName())
                .cellphone(user.getCellphone())
                .address(user.getAddress())
                .birthdate(user.getBirthdate())
                .visitor(user.isVisitor())
                .build();
    }
}