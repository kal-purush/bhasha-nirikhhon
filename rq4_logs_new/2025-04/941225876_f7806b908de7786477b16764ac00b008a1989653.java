package com.tramp.controlescolar.security.jwt;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
public class jwtUtil {
    private final Key secretKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);

    private final long expirationMillis = 3600000;

    public String generaToken(Integer id, String usuario, Integer idcategoria){
        Map<String, Object> claims = new HashMap<>();
        claims.put("id", id);
        claims.put("usuario", usuario);
        claims.put("idcategoria", idcategoria);

        return Jwts.builder()
                .setClaims(claims)
                .setSubject(usuario)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + expirationMillis))
                .signWith(secretKey)
                .compact();
    }

    public Claims extraerClaims(String token) throws JwtException{
        return Jwts.parserBuilder()
                .setSigningKey(secretKey)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }

    public boolean isTokenValid(String token, String expectedUsername){
        try{
            final String username = extraerClaims(token).getSubject();
            return username.equals(expectedUsername) && !isTokenExpired(token);
        } catch(JwtException e) {
            return false;
        }
    }

    public boolean isTokenExpired(String token){
        final Date expira = extraerClaims(token).getExpiration();
        return expira.before(new Date());
    }
}