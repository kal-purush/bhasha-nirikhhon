package adhd.diary.member.domain;

import adhd.diary.diary.common.BaseTimeEntity;
import adhd.diary.diary.domain.Diary;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Entity
public class Member extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "member_id")
    private Long id;

    private String nickname;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Role role;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SocialProvider socialProvider;

    private String socialId;

    @NotNull
    private String email;

    private LocalDate birthDay;

    private String refreshToken;

    @OneToMany(mappedBy = "member")
    private List<Diary> diaryList = new ArrayList<>();

    protected Member() {}

    public Member(Role role, SocialProvider socialProvider, String socialId, String email) {
        this.role = role;
        this.socialProvider = socialProvider;
        this.socialId = socialId;
        this.email = email;
    }

    public Long getId() {
        return id;
    }
    public String getEmail() {
        return email;
    }

    public Role getRole() {
        return role;
    }

    public String getSocialId() {
        return socialId;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public static class Builder {

        private Role role;
        private SocialProvider socialProvider;
        private String socialId;
        private String email;

        public Builder role(Role role) {
            this.role = role;
            return this;
        }

        public Builder socialProvider(SocialProvider socialProvider) {
            this.socialProvider = socialProvider;
            return this;
        }

        public Builder socialId(String socialId) {
            this.socialId = socialId;
            return this;
        }

        public Builder email(String email) {
            this.email = email;
            return this;
        }

        public Member build() {
            return new Member(role, socialProvider, socialId, email);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void updateNickname(String nickname) {
        this.nickname = nickname;
    }

    public void updateBirthDay(LocalDate birthDay) {
        this.birthDay = birthDay;
    }

    public void updateRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }
}