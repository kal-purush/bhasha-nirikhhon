package de.cookiebook.restservice.JWT;

import org.springframework.stereotype.Service;

@Service
public class JWTUtil {

    private BlockedTokenRepository blockedTokenRepository;

    private val secret = System.getenv("LIVE_POLL_JWT_SECRET")

    fun extractUsername(token: String?): String = extractClaim(token) { obj: Claims -> obj.subject }

    fun extractExpiration(token: String?): Date = extractClaim(token) { obj: Claims -> obj.expiration }

    fun <T> extractClaim(token: String?, claimsResolver: Function<Claims, T>) = claimsResolver.apply(extractAllClaims(token))

    private fun extractAllClaims(token: String?): Claims {
        return Jwts.parser().setSigningKey(secret)
                .parseClaimsJws(token)
                .body
    }

    private fun isTokenExpired(token: String?) = extractExpiration(token).before(Date())

    fun generateToken(userDetails: UserDetails) = createToken(mutableMapOf(), userDetails.username)

    private fun createToken(claims: Map<String, Any>, subject: String): String {
        return Jwts.builder()
                .setClaims(claims)
                .setSubject(subject).setIssuedAt(Date(System.currentTimeMillis()))
                .setExpiration(Date(System.currentTimeMillis() + TOKEN_DURATION * 1000))
                .signWith(SignatureAlgorithm.HS256, secret).compact()
    }

    fun validateToken(token: String?, userDetails: UserDetails)
            = extractUsername(token) == userDetails.username && !isTokenExpired(token) && !isTokenBlocked(token)

    private fun isTokenBlocked(token: String?): Boolean {
        blockedTokenRepository.findByToken(token)?.run {
            return true
        }
        return false
    }
}