package com.example.apigatewayservice.filter;

import org.apache.commons.lang.StringUtils;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import com.ctc.wstx.util.StringUtil;

import io.jsonwebtoken.Jwts;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class AuthorizationHeaderFilter extends AbstractGatewayFilterFactory<AuthorizationHeaderFilter.Config> {
	private final Environment env;

	public AuthorizationHeaderFilter(Environment env) {
		super(AuthorizationHeaderFilter.Config.class);
		this.env = env;
	}


	@Override
	public GatewayFilter apply(final Config config) {
		return (exchange, chain) -> {
			ServerHttpRequest request = exchange.getRequest();

			if (!request.getHeaders().containsKey(HttpHeaders.AUTHORIZATION)) {
				return onError(exchange, "no AUTHORIZATION header", HttpStatus.UNAUTHORIZED);
			}

			String authorizationHeader = request.getHeaders().get(HttpHeaders.AUTHORIZATION).get(0);
			if (!authorizationHeader.startsWith("Bearer ")) {
				return onError(exchange, "no bearer token", HttpStatus.UNAUTHORIZED);
			}
			String jwt = authorizationHeader.substring(7);
			if (!isValidJwt(jwt)) {
				return onError(exchange, "invalid token", HttpStatus.UNAUTHORIZED);
			}

			return chain.filter(exchange);
		};
	}

	private boolean isValidJwt(final String jwt) {
		boolean result = true;
		String subject = null;

		try{
			subject = Jwts.parser()
				.setSigningKey(env.getProperty("jwt.secret"))
				.parseClaimsJws(jwt).getBody()
				.getSubject();

		}catch (Exception ex){
			result = false;
		}

		if(StringUtils.isEmpty(subject)){
			result = false;
		}

		return result;
	}

	private Mono<Void> onError(final ServerWebExchange exchange, final String errorMessage, final HttpStatus status) {

		ServerHttpResponse response = exchange.getResponse();
		response.setStatusCode(status);
		log.info("errorMessage : {}", errorMessage);
		return response.setComplete();
	}

	static class Config {
	}
}