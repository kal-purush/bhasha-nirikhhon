package com.engineCore.UPDATESERVICE.DTO;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Pattern(regexp = "\\S+", message = "No es posible agregar espacios a los campos")
public class UserDto {

    private String username;

    private String password;

    private String firstName;

    private String middleName;

    private String lastName;

    @Email(message = "El formato del email no es válido")
    private String email;
}