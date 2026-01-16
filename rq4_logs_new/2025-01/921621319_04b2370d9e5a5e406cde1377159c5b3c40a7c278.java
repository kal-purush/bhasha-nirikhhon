package com.metamong.mt.member.dto.request;

import java.time.LocalDate;

import com.metamong.mt.member.model.Member;
import com.metamong.mt.member.model.Role;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class OwnerSignUpRequestDto {
    private String userId;
    private String email;
    private String password;
    private String confirmPassword;
    private String name;
    private String phone;
    private String businessName;
    private String businessRegistrationNumber;
    private LocalDate birth;
    private String postalCode;
    private String detailAddress;
    private String address;
    
    public Member toEntity() {
        return Member.builder()
                .userId(this.userId)
                .email(this.email)
                .password(this.password)
                .name(this.name)
                .phone(this.phone)
                .businessName(this.businessName)
                .businessRegistrationNumber(this.businessRegistrationNumber)
                .birth(this.birth)
                .postalCode(this.postalCode)
                .detailAddress(this.detailAddress)
                .address(this.address)
                .role(Role.ROLE_OWNER)
                .build();
    }
}