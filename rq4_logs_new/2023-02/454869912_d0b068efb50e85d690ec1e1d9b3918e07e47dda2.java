package it.pagopa.pm.gateway.utils;

import io.jsonwebtoken.Jwts;
import org.springframework.stereotype.Component;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import static io.jsonwebtoken.SignatureAlgorithm.HS256;
import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class JwtTokenUtils {

    public static final String REQUEST_ID = "requestId";
    public static final int TOKEN_VALIDITY_MS = 600000;

    public String generateToken(String requestId) {
        Calendar calendar = Calendar.getInstance();
        Date issueDate = calendar.getTime();
        calendar.add(Calendar.MILLISECOND, TOKEN_VALIDITY_MS);
        Date expiryDate = calendar.getTime();

        return Jwts.builder()
                .claim(REQUEST_ID, requestId)
                .setId(UUID.randomUUID().toString())
                .setIssuedAt(issueDate)
                .setExpiration(expiryDate)
                .signWith(HS256, requestId.getBytes(UTF_8))
                .compact();
    }
}