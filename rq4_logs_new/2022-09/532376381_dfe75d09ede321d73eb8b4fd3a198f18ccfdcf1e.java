package com.example.sbemintic.dtos.request;

import lombok.*;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateUserRequestDto {
    @NotEmpty()
    @NotNull()
    @Size(min = 2)
    private String seudonimo;
    @NotNull()
    @Email(message = "Correo no valido")
    private String mail;
    @Size(min = 8)
    @NotNull()
    private String password;

}