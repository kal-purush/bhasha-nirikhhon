package local.socialnetwork.apigateway.util;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.SignatureAlgorithm;

import jakarta.annotation.PostConstruct;

import lombok.AccessLevel;

import lombok.experimental.FieldDefaults;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.stereotype.Component;

import javax.crypto.spec.SecretKeySpec;

import java.security.Key;

import java.util.Date;
import java.util.Base64;

@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
public class JwtUtils {

    @Value("${security.jwt.token.secret-key}")
    String secretKey;

    Key key;

    @PostConstruct
    protected void init() {
        key = new SecretKeySpec(Base64.getDecoder().decode(secretKey),
                SignatureAlgorithm.HS256.getJcaName());
    }

    public boolean isTokenExpired(String token) {
        return extractClaims(token).getExpiration().before(new Date());
    }

    private Claims extractClaims(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }
}