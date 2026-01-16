package com.umc.bwither.member.dto;

import com.umc.bwither.member.entity.Member;
import com.umc.bwither.member.entity.enums.EmploymentStatus;
import com.umc.bwither.member.entity.enums.FamilyAgreement;
import com.umc.bwither.member.entity.enums.FuturePlan;
import com.umc.bwither.member.entity.enums.PetAllowed;
import com.umc.bwither.user.entity.enums.Role;
import com.umc.bwither.user.entity.enums.Status;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@Data
public class JoinDto {

    //user
    private Long userId;
    private String name;
    private String phone;
    private String email;
    private String username;
    private String password;
    private String zipcode;
    private String address;
    private String addressDetail;
    private String profileImage;
    private Role role;
    private Status status;
    private LocalDateTime createdAt;

    //member
    private PetAllowed petAllowed;
    private String cohabitant;
    private FamilyAgreement familyAgreement;
    private EmploymentStatus employmentStatus;
    private String commuteTime;
    private Boolean petExperience;
    private String currentPet;
    private FuturePlan futurePlan;

    public Member toMember() {
        Member member = new Member();
        member.setUserId(this.userId);
        member.setName(this.name);
        member.setPhone(this.phone);
        member.setEmail(this.email);
        member.setUsername(this.username);
        member.setPassword(this.password);
        member.setZipcode(this.zipcode);
        member.setAddress(this.address);
        member.setAddressDetail(this.addressDetail);
        member.setProfileImage(this.profileImage);
        member.setRole(this.role);
        member.setStatus(this.status);
        member.setCreatedAt(this.createdAt);
        member.setPetAllowed(this.petAllowed);
        member.setCohabitant(this.cohabitant);
        member.setFamilyAgreement(this.familyAgreement);
        member.setEmploymentStatus(this.employmentStatus);
        member.setCommuteTime(this.commuteTime);
        member.setPetExperience(this.petExperience);
        member.setCurrentPet(this.currentPet);
        member.setFuturePlan(this.futurePlan);
        return member;
    }
}