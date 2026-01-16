package com.sp.fc.web.config;

import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;


@EnableWebSecurity(debug = true) // SpringSecurityFilterChain 이 자동으로 포함됨
@EnableGlobalMethodSecurity(prePostEnabled = true) // 사전에 prePost 를 통해 권한 체크
public class SecurityConfig extends WebSecurityConfigurerAdapter{

    // 간단하게 유저 정보 추가 가능
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.inMemoryAuthentication()
                .withUser(User.builder()
                        .username("user2")
                        .password(passwordEncoder().encode("2222"))
                        .roles("USER"))
                .withUser(User.builder()
                        .username("user3")
                        .password(passwordEncoder().encode("3333"))
                        .roles("ADMIN"))
                ;
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }


    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeRequests((requests) ->
                requests
                        .antMatchers("/").permitAll() // 모든 사용자에게 권한 열기
                        .anyRequest().authenticated());
        http.formLogin();
        http.httpBasic();
    }
}