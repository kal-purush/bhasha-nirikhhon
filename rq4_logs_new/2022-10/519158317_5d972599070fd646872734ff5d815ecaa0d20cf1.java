package com.example.chatapi.Entity.User;

import lombok.*;

import javax.persistence.*;

@Entity
@Table(name = "OAUTH2_USER")
@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OAuth2UserEntity {

    @Id
    private Long id;

    @ManyToOne
    @JoinColumn(name = "oauth2_type")
    private OAuth2Entity oauth2Type;

    @Column
    private String email;

    @Column
    private String nickname;

    public void setId(Long id) {
        this.id = id;
    }

    public void setOauth2Type(OAuth2Entity oauth2Type) {
        this.oauth2Type = oauth2Type;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
}