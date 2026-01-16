package com.example.demo.userapi.dto.request;

import com.example.demo.userapi.entity.User;
import lombok.*;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@Getter
@ToString @EqualsAndHashCode
@NoArgsConstructor @AllArgsConstructor
@Builder
public class UserModifyRequestDTO {

    @Size(min = 8, max = 20)
    private String password;

    private String address;


    public User toEntity(){
        return User.builder()
                .password(this.password)
                .userAddress(this.address)
                .build();
    }
}