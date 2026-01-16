package com.cibertec.proyecto.integrador.config;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.security.Keys;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;

@Component

public class JWTProvider {
    private final SecretKey SECRET_KEY ;
    public JWTProvider() {
        // Genera una clave segura de 512 bits para HS512
        this.SECRET_KEY = Keys.secretKeyFor(SignatureAlgorithm.HS512);
    }
    public String generateToken(String username, List<SimpleGrantedAuthority> authorities) {
        Date now = new Date();
        Date expirationDate = new Date(now.getTime() + 86400000);

        return Jwts.builder()
                .setSubject(username)
                .claim("roles", authorities)  // Agrega los roles del usuario al token

                .setIssuedAt(now)
                .setExpiration(expirationDate)
                .signWith(SignatureAlgorithm.HS512, SECRET_KEY)
                .compact();
    }

    public String getUsernameFromToken(String token) {
        Claims claims = Jwts.parser()
                .setSigningKey(SECRET_KEY)
                .parseClaimsJws(token)
                .getBody();
        return claims.getSubject();
    }

    public boolean validateToken(String token) {
        try {

            Jwts.parser().setSigningKey(SECRET_KEY).parseClaimsJws(token);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Authentication getAuthentication(String token) {
        String username = getUsernameFromToken(token);
        List<SimpleGrantedAuthority> authorities = extractRolesFromToken(token);  // Método para extraer roles del token
        return new UsernamePasswordAuthenticationToken(username, null, authorities);
    }

    private List<SimpleGrantedAuthority> extractRolesFromToken(String token) {
        Claims claims = Jwts.parser()
                .setSigningKey(SECRET_KEY)
                .parseClaimsJws(token)
                .getBody();

        List<Map<String, String>> roles = (List<Map<String, String>>) claims.get("roles");

        // Mapea los roles del token a SimpleGrantedAuthority
        return roles.stream()
                .map(role -> new SimpleGrantedAuthority(role.get("authority")))
                .collect(Collectors.toList());
    }


}