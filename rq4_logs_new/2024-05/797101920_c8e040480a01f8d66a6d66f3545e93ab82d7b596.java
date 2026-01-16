package com.example.EnjoyTripBackend.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Member extends BaseTimeDomain{

    private Long id;
    private String email;
    private String username;
    private String password;
    private Integer age;
    private Gender gender;
    private String phoneNumber;
    private MemberRole memberRole;

}