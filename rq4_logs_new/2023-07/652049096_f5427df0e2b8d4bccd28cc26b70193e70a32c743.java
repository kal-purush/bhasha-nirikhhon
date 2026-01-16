package com.bahubba.bahubbabookclub.service;

import com.bahubba.bahubbabookclub.model.dto.MessageResponseDTO;
import com.bahubba.bahubbabookclub.model.entity.RefreshToken;
import io.jsonwebtoken.Claims;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public interface JwtService {
    ResponseCookie generateJwtCookie(UserDetails userDetails);

    ResponseCookie generateJwtRefreshCookie(String refreshToken);

    String getJwtFromCookies(HttpServletRequest req);

    String getJwtRefreshFromCookies(HttpServletRequest req);

    ResponseCookie getCleanJwtCookie();

    ResponseCookie getCleanJwtRefreshCookie();

    String extractUsername(String token);

    String generateToken(UserDetails userDetails);

    String generateToken(Map<String, Object> extraClaims, UserDetails userDetails);

    boolean isTokenValid(String token, UserDetails userDetails);

    <T> T extractClaim(String token, Function<Claims, T> claimsResolver);

    ResponseEntity<MessageResponseDTO> refreshToken(String token);

    Optional<RefreshToken> getByToken(String token);

    RefreshToken createRefreshToken(UUID readerID);

    RefreshToken verifyExpiration(RefreshToken token);

    int deleteByReaderID(UUID readerID);
}