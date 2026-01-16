package com.flexiorder.security.config.dtos;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

@Data
public class LoginRequestDTO {
    @Length(min = 3, max = 50)
    @NotEmpty(message = "Campo obrigatório")
    @JsonProperty("email")
    private String email;

    @Length(min = 5, max = 50)
    @NotEmpty(message = "Campo obrigatório")
    private String password;
}