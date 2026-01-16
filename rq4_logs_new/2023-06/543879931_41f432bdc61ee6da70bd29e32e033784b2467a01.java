package com.example.demo.application;

import com.example.demo.infrastructor.UserDetailsDao;
import com.example.demo.utils.AccessTokenGenerator;
import jakarta.transaction.Transactional;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@Transactional
public class LoginService {
    private final UserDetailsDao userDetailsDao;
    private final AccessTokenGenerator accessTokenGenerator;
    private final PasswordEncoder passwordEncoder;

    public LoginService(UserDetailsDao userDetailsDao,
                        AccessTokenGenerator accessTokenGenerator,
                        PasswordEncoder passwordEncoder) {
        this.userDetailsDao = userDetailsDao;
        this.accessTokenGenerator = accessTokenGenerator;
        this.passwordEncoder = passwordEncoder;
    }

    public String login(String username, String password) {
        return userDetailsDao.findByUsername(username)
            .filter(userDetails ->
                passwordEncoder.matches(
                    password, userDetails.getPassword()))
            .map(userDetails -> {
                String id = userDetails.getUsername();
                String accessToken = accessTokenGenerator.generate(id);
                userDetailsDao.addAccessToken(id, accessToken);
                return accessToken;
            })
            .orElseThrow(() -> new BadCredentialsException("Login failed!"));
    }
}