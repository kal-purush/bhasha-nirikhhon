package com.kraft.atend.aspects;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.kraft.atend.annotations.JwtFilter;

import io.jsonwebtoken.Jwts;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@Aspect
@Component
@RequiredArgsConstructor
public class JwtFilterAspect {

	@Value("${jwt.secret.key}")
	private final String SECRET_KEY;
	private final HttpServletRequest httpServletRequest;

	@Before("@annotation(JwtFilter)")
	public void CheckEndpointAnnotation(JwtFilter jwtFilter) {
		var authHeader = httpServletRequest.getHeader("Authorization");
		var token = authHeader.substring(7);

		try {
			Jwts.parser().setSigningKey(SECRET_KEY).parseClaimsJws(token).getBody();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}

	}

}