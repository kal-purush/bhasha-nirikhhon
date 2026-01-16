package com.chapssal_tteok.preview.domain.user.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

@Entity
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RefreshToken {

    @Id
    private String username;

    @Column(nullable = false)
    private String refreshToken;

    // update refresh token
    public void updateRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }
}