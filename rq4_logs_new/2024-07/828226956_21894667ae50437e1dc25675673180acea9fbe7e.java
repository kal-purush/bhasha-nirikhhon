package adhd.diary.member.dto;

import adhd.diary.member.domain.Role;
import adhd.diary.member.domain.SocialProvider;

import java.time.LocalDate;

public class MemberResponse {

    private String email;
    private String nickname;
    private LocalDate birthDay;
    private Role role;
    private String socialId;
    private SocialProvider socialProvider;

    public MemberResponse(String email, String nickname, LocalDate birthDay, Role role, String socialId, SocialProvider socialProvider) {
        this.email = email;
        this.nickname = nickname;
        this.birthDay = birthDay;
        this.role = role;
        this.socialId = socialId;
        this.socialProvider = socialProvider;
    }
}