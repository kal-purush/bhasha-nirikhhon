package com.spring.boot.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.lang.Nullable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AdminDTO {

    @Nullable
    private String id;
    private String password;

    @Override
    public String toString() {
        return "AdminDTO{" +
                "id='" + id + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}