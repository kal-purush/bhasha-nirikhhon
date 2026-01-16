package com.dateplan.dateplan.global.auth;

import static com.dateplan.dateplan.global.constant.Auth.HEADER_AUTHORIZATION;

import com.dateplan.dateplan.domain.member.entity.Member;
import com.dateplan.dateplan.domain.member.repository.MemberRepository;
import com.dateplan.dateplan.global.exception.auth.MemberNotFoundException;
import com.dateplan.dateplan.global.exception.auth.TokenExpiredException;
import com.dateplan.dateplan.global.exception.auth.TokenInvalidException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class JwtProvider {

	@Value("${jwt.secret}")
	private String secret;
	private final MemberRepository memberRepository;

	public Member findMemberByToken(String token) {
		return memberRepository.findById(getIdByToken(token))
			.orElseThrow(MemberNotFoundException::new);
	}

	private Long getIdByToken(String token) {
		try {
			return (Long) Jwts.parser()
				.setSigningKey(generateKey())
				.parseClaimsJws(token)
				.getBody()
				.get("id");
		} catch (ExpiredJwtException e) {
			throw new TokenExpiredException();
		} catch (MalformedJwtException | SignatureException | IllegalArgumentException e) {
			throw new TokenInvalidException();
		}
	}

	public String generateToken(Long id, Long expiration, String subject) {
		Date issueDate = new Date();
		Date expireDate = new Date();
		expireDate.setTime(issueDate.getTime() + expiration);
		return Jwts.builder()
			.setHeaderParam("typ", "JWT")
			.setClaims(generateClaims(id))
			.setIssuedAt(issueDate)
			.setSubject(subject)
			.setExpiration(expireDate)
			.signWith(SignatureAlgorithm.HS256, generateKey())
			.compact();
	}

	private Claims generateClaims(Long id) {
		Claims claims = Jwts.claims();
		claims.put("id", id);
		return claims;
	}

	public boolean isValid(String token) {
		try {
			Jwts.parser()
				.setSigningKey(generateKey())
				.parseClaimsJws(token);
			return true;
		} catch (ExpiredJwtException e) {
			throw new TokenExpiredException();
		} catch (MalformedJwtException | SignatureException | IllegalArgumentException e) {
			throw new TokenInvalidException();
		}
	}

	public Optional<String> resolveToken(HttpServletRequest request) {
		return Optional.ofNullable(request.getHeader(HEADER_AUTHORIZATION.getContent()));
	}

	private byte[] generateKey() {
		return secret.getBytes(StandardCharsets.UTF_8);
	}
}