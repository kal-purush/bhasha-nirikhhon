package com.coe.andaman.as.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.config.annotation.configurers.ClientDetailsServiceConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configuration.AuthorizationServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerSecurityConfigurer;

public abstract class BaseAuthConfig extends AuthorizationServerConfigurerAdapter {
	@Value("${app.oauth2.realm}")
	private String REALM;
	@Value("${app.oauth2.client.client-id}")
	private String CLIEN_ID;
	@Value("${app.oauth2.client.client-pwd}")
	private String CLIENT_SECRET;

	static final String GRANT_TYPE_PASSWORD = "password";
	static final String AUTHORIZATION_CODE = "authorization_code";
	static final String REFRESH_TOKEN = "refresh_token";
	static final String IMPLICIT = "implicit";
	static final String SCOPE_READ = "read";
	static final String SCOPE_WRITE = "write";
	static final String TRUST = "trust";

	/*
	 * @Value("${app.oauth2.clint.authorized-grant-types}") private List<String>
	 * GRANT_TYPES ;
	 * 
	 * @Value("${app.oauth2.clint.scope}") private List<String> SCOPE ;
	 * 
	 * @Value("${app.oauth2.clint.authorities}") private List<String> AUTHORIRIES ;
	 */
	/*
	*/
	@Value("${app.oauth2.client.token-validity}")
	private int ACCESS_TOKEN_VALIDITY_SECONDS;// = 1*60*60;
	@Value("${app.oauth2.client.refresh-token-validity}")
	private int FREFRESH_TOKEN_VALIDITY_SECONDS;// = 6*60*60;

	@Override
	public void configure(AuthorizationServerSecurityConfigurer oauthServer) throws Exception {
		oauthServer.realm(REALM);
	}

	@Override
	public void configure(ClientDetailsServiceConfigurer clients) throws Exception {
		clients.inMemory().withClient(CLIEN_ID).secret(CLIENT_SECRET)
				.authorizedGrantTypes(GRANT_TYPE_PASSWORD, AUTHORIZATION_CODE, REFRESH_TOKEN, IMPLICIT)
				.authorities("ROLE_CLIENT", "ROLE_TRUSTED_CLIENT").scopes(SCOPE_READ, SCOPE_WRITE, TRUST)
				.accessTokenValiditySeconds(ACCESS_TOKEN_VALIDITY_SECONDS)
				.refreshTokenValiditySeconds(FREFRESH_TOKEN_VALIDITY_SECONDS);
	}

}