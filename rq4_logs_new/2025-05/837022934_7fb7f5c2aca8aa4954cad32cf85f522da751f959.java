package leets.weeth.global.sas.config;

import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import leets.weeth.domain.user.domain.entity.SecurityUser;
import leets.weeth.global.config.ProviderAwareEntryPoint;
import leets.weeth.global.sas.application.mapper.OAuth2AuthorizationMapper;
import leets.weeth.global.sas.domain.repository.OAuth2AuthorizationGrantAuthorizationRepository;
import leets.weeth.global.sas.domain.service.RedisOAuth2AuthorizationService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.oidc.OidcScopes;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.client.InMemoryRegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.config.annotation.web.configuration.OAuth2AuthorizationServerConfiguration;
import org.springframework.security.oauth2.server.authorization.config.annotation.web.configurers.OAuth2AuthorizationServerConfigurer;
import org.springframework.security.oauth2.server.authorization.settings.AuthorizationServerSettings;
import org.springframework.security.oauth2.server.authorization.settings.TokenSettings;
import org.springframework.security.oauth2.server.authorization.token.JwtEncodingContext;
import org.springframework.security.oauth2.server.authorization.token.OAuth2TokenCustomizer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;

import java.security.PrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;

@Configuration
@RequiredArgsConstructor
public class OAuth2AuthorizationServerConfig {
    private static final Logger log = LoggerFactory.getLogger(OAuth2AuthorizationServerConfig.class);
    private final ProviderAwareEntryPoint entryPoint;

    private final RSAPublicKey publicKey;
    private final PrivateKey privateKey;

    // SAS용 SecurityFilterChain
    @Bean
    @Order(1) // 우선순위를 기본 filter보다 높게 설정
    public SecurityFilterChain authorizationServerSecurityFilterChain(HttpSecurity http) throws Exception {
        OAuth2AuthorizationServerConfiguration.applyDefaultSecurity(http);

        http.getConfigurer(OAuth2AuthorizationServerConfigurer.class)
                .oidc(Customizer.withDefaults());

        http.oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()));

        // 커스텀 EntryPoint (provider 파라미터 해석 → 302 /oauth2/authorization/{provider})
        http.exceptionHandling(e -> e.defaultAuthenticationEntryPointFor(
                entryPoint, rq -> rq.getRequestURI().startsWith("/oauth2/authorize")));

        return http
                .csrf(csrf -> csrf.ignoringRequestMatchers("/oauth2/**", "/.well-known/**"))
                .build();
    }

    // Client 저장소 설정 -> JPA로 이전
    @Bean
    public RegisteredClientRepository registeredClientRepository() {
        RegisteredClient client = RegisteredClient.withId("39153362-3a3c-4723-8538-d7684dad3fcb") // 고정 id
                .clientName("LEENK") //임시값
                .clientId("leenk-client") //임시값
                .clientSecret("{noop}your-secret") //임시값
                .authorizationGrantTypes(type -> {
                    type.add(AuthorizationGrantType.AUTHORIZATION_CODE);
                    type.add(AuthorizationGrantType.REFRESH_TOKEN);
                })
                .redirectUris(uri -> {
                    uri.add("http://localhost:3000"); //임시값
                })
                .scopes(scope -> {
                    scope.add(OidcScopes.OPENID);
                    scope.add(OidcScopes.PROFILE);
                })
                .tokenSettings(TokenSettings.builder()
                        .accessTokenTimeToLive(Duration.ofHours(1))
                        .refreshTokenTimeToLive(Duration.ofHours(144))
                        .reuseRefreshTokens(false)
                        .build())
                .build();
        return new InMemoryRegisteredClientRepository(client);
    }

    @Bean
    @Primary
    public OAuth2AuthorizationService authorizationService(
            RegisteredClientRepository registeredClientRepository,
            OAuth2AuthorizationGrantAuthorizationRepository oAuth2AuthorizationGrantAuthorizationRepository,
            OAuth2AuthorizationMapper oAuth2AuthorizationMapper) {

        return new RedisOAuth2AuthorizationService(registeredClientRepository, oAuth2AuthorizationGrantAuthorizationRepository, oAuth2AuthorizationMapper);
    }

    @Bean
    public SecurityContextRepository securityContextRepository() {
        return new HttpSessionSecurityContextRepository();
    }

    @Bean
    SavedRequestAwareAuthenticationSuccessHandler savedHandler() {
        return new SavedRequestAwareAuthenticationSuccessHandler();
    }

    // SAS에서 사용하는 RSA 키쌍을 제공
    @Bean
    public JWKSource<SecurityContext> jwkSource() {
        RSAKey rsaKey = loadRsaKeyFromString();
        return (jwkSelector, context) -> jwkSelector.select(new com.nimbusds.jose.jwk.JWKSet(rsaKey));
    }

    // OIDC 메타데이터에서 issuer를 명시해줘야 client가 자동 설정 가능
    @Bean
    public AuthorizationServerSettings authorizationServerSettings() {
        return AuthorizationServerSettings.builder()
                .issuer("https://weeth.site") //임시값
                .build();
    }

    @Bean
    public OAuth2TokenCustomizer<JwtEncodingContext> jwtCustomizer() {
        return ctx -> {
            Object principal = ctx.getPrincipal().getPrincipal();

            if (principal instanceof SecurityUser u) {
                ctx.getClaims().claim("id", u.id());
                ctx.getClaims().claim("email", u.email());
                ctx.getClaims().claim("role", u.role());
            }else {
                log.info("토큰 생성 안됨");
            }
        };
    }

    private RSAKey loadRsaKeyFromString() {
        try {
            return new RSAKey.Builder(publicKey)
                    .privateKey(privateKey)
                    .keyID("weeth-key")
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("RSA 키 파싱 실패", e);
        }
    }
}