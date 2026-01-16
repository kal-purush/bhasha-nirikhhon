package leets.weeth.global.sas.domain.entity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;

import java.security.Principal;

@Getter
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OAuth2AuthorizationCodeGrantAuthorization extends OAuth2AuthorizationGrantAuthorization {

    private Principal principal;

    private OAuth2AuthorizationRequest authorizationRequest;

    private AuthorizationCode authorizationCode;

}