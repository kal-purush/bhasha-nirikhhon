package com.example.ordermanagement.domain.model.entity;

import com.example.ordermanagement.domain.model.enumClass.GradeStatus;
import lombok.*;
import java.time.LocalDate;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class CustomerDto {

    private Long seq;

    private String userId;

    private String password;

    private String name;

    private String nickname;

    private LocalDate birthday;

    private String phoneNumber;

    private String email;

    private String address;

    private GradeStatus grade;

    private Integer role;

    private LocalDate registeredAt;
}