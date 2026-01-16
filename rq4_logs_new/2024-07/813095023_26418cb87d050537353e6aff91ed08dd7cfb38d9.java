package com.spring.mood.projectmvc.dto.responseDto;

import com.spring.mood.projectmvc.dto.requestDto.RequestMyPageMemberInfoDto;
import com.spring.mood.projectmvc.entity.Member;
import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.LocalDate;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResponseMyPageMemberInfoDto {

    private String account; // 유저 아이디

    private String name; // 유저 이름

    private String email; // 유저 이메일

    private LocalDate birth; // 유저 생년월일

    private String profileImage;  // 유저 프로필사진

    public ResponseMyPageMemberInfoDto(Member member) {
        this.account = member.getUserAccount();
        this.name = member.getUserName();
        this.email = member.getUserEmail();
        this.birth = member.getUserBirth();
        this.profileImage = member.getUserProfile();
    }

    // RequestMyPageMemberInfoDto 를 MemberEntity 로 변환
    public Member toMemberEntity(String account, RequestMyPageMemberInfoDto dto) {
        return Member.builder().userAccount(account).userName(dto.getName())
                .userEmail(dto.getEmail()).userBirth(dto.getBirth()).userProfile(dto.getProfileImage()).build();
    }
}