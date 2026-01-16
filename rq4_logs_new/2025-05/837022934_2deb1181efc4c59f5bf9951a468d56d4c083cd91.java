package leets.weeth.global.sas.domain.repository;

import leets.weeth.global.sas.domain.entity.OAuth2AuthorizationCodeGrantAuthorization;
import leets.weeth.global.sas.domain.entity.OAuth2AuthorizationGrantAuthorization;
import leets.weeth.global.sas.domain.entity.OidcAuthorizationCodeGrantAuthorization;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OAuth2AuthorizationGrantAuthorizationRepository extends CrudRepository<OAuth2AuthorizationGrantAuthorization, String> {

    <T extends OAuth2AuthorizationCodeGrantAuthorization> T findByAuthorizationCode_TokenValue(String authorizationCode);

    <T extends OAuth2AuthorizationCodeGrantAuthorization> T findByStateOrAuthorizationCode_TokenValue(String state, String authorizationCode);

    <T extends OAuth2AuthorizationGrantAuthorization> T findByAccessToken_TokenValue(String accessToken);

    <T extends OAuth2AuthorizationGrantAuthorization> T findByRefreshToken_TokenValue(String refreshToken);

    <T extends OAuth2AuthorizationGrantAuthorization> T findByAccessToken_TokenValueOrRefreshToken_TokenValue(String accessToken, String refreshToken);

    <T extends OidcAuthorizationCodeGrantAuthorization> T findByIdToken_TokenValue(String idToken);

}
