package com.example.auction.User.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;

@Getter
public class JoinRequestDto {

    @NotBlank(message = "email은 빈 값이 허용되지 않습니다.")
    @Email(message = "올바른 email 형식이 아닙니다.")
    private final String email;

    /**
     * 암호.
     */
    @NotBlank(message = "password는 빈 값이 허용되지 않습니다.")
    private final String password;

    @NotBlank(message = "name은 빈 값이 허용되지 않습니다.")
    private final String name;

    @NotBlank(message = "phone은 빈 값이 허용되지 않습니다.")
    private final String phone;

    /**
     * 생성자.
     *
     * @param email    이메일
     * @param password 암호
     */
    public JoinRequestDto(String email, String password, String name, String phone) {
        this.email = email;
        this.password = password;
        this.name = name;
        this.phone = phone;
    }
}