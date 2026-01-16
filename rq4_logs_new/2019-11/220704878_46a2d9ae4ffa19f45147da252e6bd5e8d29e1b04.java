package com.rest.springboot.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.oauth2.config.annotation.configurers.ClientDetailsServiceConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configuration.AuthorizationServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableAuthorizationServer;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerEndpointsConfigurer;
import org.springframework.security.oauth2.provider.token.TokenStore;
import org.springframework.security.oauth2.provider.token.store.JwtAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.store.JwtTokenStore;

@Configuration
@EnableAuthorizationServer
public class AuthorizationServerConfig extends AuthorizationServerConfigurerAdapter {

	@Autowired
	private AuthenticationManager authenticationManager;
	
	 @Override
	 public void configure(ClientDetailsServiceConfigurer clients) throws Exception {
		 clients.inMemory()
		 .withClient("angular")
		 .secret("!4NGUL4R!")
		 .scopes("read", "write")
		 .authorizedGrantTypes("password", "refresh_token") //password flow
		 .accessTokenValiditySeconds(50)
		 .refreshTokenValiditySeconds(3600*24);
		 
	}
	 
	 @Override
	 public void configure(AuthorizationServerEndpointsConfigurer endpoints) throws Exception {
		 endpoints
		 .tokenStore(tokenStore())
		 .accessTokenConverter(accessTokenConverter())
		 .reuseRefreshTokens(false) //nao reaproveitar!!
		 .authenticationManager(this.authenticationManager);
	}
	 
	 @Bean
	 public JwtAccessTokenConverter accessTokenConverter() {
		JwtAccessTokenConverter accessTokenConverter = new JwtAccessTokenConverter();
		accessTokenConverter.setSigningKey("springboot");
		return accessTokenConverter;
	}
	
	 @Bean
	 public TokenStore tokenStore() {
		 //return new InMemoryTokenStore();
		 return new JwtTokenStore(accessTokenConverter());
	 }
}