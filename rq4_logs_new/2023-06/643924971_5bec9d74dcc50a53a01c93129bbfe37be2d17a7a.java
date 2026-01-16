package com.example.ticketing.service;

import com.example.ticketing.user.AuthenticationResult;
import com.example.ticketing.user.LoginRequest;
import com.example.ticketing.user.LoginResponse;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Date;

public class LoginService {

    public ResponseEntity<LoginResponse> login(LoginRequest loginRequest){
        String username = loginRequest.getUsername();
        String password = loginRequest.getPassword();

        AuthenticationResult result = authenticateUser(username, password);

        if (result.isAuthenticated()) {
            LoginResponse response = new LoginResponse(result.getToken());
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
    }
    public AuthenticationResult authenticateUser(String username,String password){
        //to implement IDProvider connection after Registration implementation
        boolean isAuthenticated=true;
        if (isAuthenticated){
            String token=generateToken(username);
            return new AuthenticationResult(true,token);
        }else {
            return new AuthenticationResult(false,null);
        }
    }

    public String generateToken(String userId) {
        long expirationTime = 3600000; // 1 hour in milliseconds
        String secretKey = "yourSecretKey";

        return Jwts.builder()
                .setSubject(userId)
                .setExpiration(new Date(System.currentTimeMillis() + expirationTime))
                .signWith(SignatureAlgorithm.HS512, secretKey)
                .compact();
    }

    public boolean validateToken(String token) {
        String secretKey = "yourSecretKey";

        try {
            Jwts.parserBuilder()
                    .setSigningKey(secretKey)
                    .build()
                    .parseClaimsJws(token);

            return true;
        } catch (Exception e) {
            // Token validation failed
            return false;
        }
    }

    public String getUserIdFromToken(String token) {
        String secretKey = "yourSecretKey";

        try {
            Claims claims = Jwts.parserBuilder()
                    .setSigningKey(secretKey)
                    .build()
                    .parseClaimsJws(token)
                    .getBody();

            return claims.getSubject();
        } catch (Exception e) {
            // Token validation failed
            return null;
        }
    }
}