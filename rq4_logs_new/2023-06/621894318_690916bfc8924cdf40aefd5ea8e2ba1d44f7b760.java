package com.gxdxx.instagram.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;
import java.util.List;

// UserDetails: 스프링 시큐리티에서 사용자의 인증 정보를 담아두는 인터페이스
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UserDetailsImpl implements UserDetails {

    private User user;

    public static UserDetailsImpl of(User user) {
        return new UserDetailsImpl(user);
    }

    // 권한 반환
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return List.of(new SimpleGrantedAuthority("user"));
    }

    // 사용자의 닉네임을 반환
    @Override
    public String getUsername() {
        return this.user.getNickname();
    }

    // 사용자의 패스워드를 반환
    @Override
    public String getPassword() {
        return this.user.getPassword();
    }

    // 계정 만료 여부 반환
    @Override
    public boolean isAccountNonExpired() {
        // 계정 만료되었는지 확인하는 로직 추가 필요
        return true; // true -> 만료되지 않았음
    }

    // 계정 잠금 여부 반환
    @Override
    public boolean isAccountNonLocked() {
        // 계정 잠금되었는지 확인하는 로직 추가 필요
        return true; // true -> 잠금되지 않았음
    }

    // 패스워드 만료 여부 반환
    @Override
    public boolean isCredentialsNonExpired() {
        // 패스워드 만료되었는지 확인하는 로직 추가 필요
        return true; // true -> 만료되지 않았음
    }

    // 계정 사용 가능 여부 반환
    @Override
    public boolean isEnabled() {
        // 계정 사용 가능한지 확인하는 로직 추가 필요
        return true; // true -> 사용 가능함
    }

}