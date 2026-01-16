package de.cuioss.portal.authentication.token;

import de.cuioss.tools.logging.CuiLogger;
import io.smallrye.jwt.auth.principal.DefaultJWTParser;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTParser;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Delegate;

/**
 * Variant of {@link JWTParser} that will be configured for remote loading of the public-keys.
 * They are needed to verify the signature or the token.
 *
 * @author Oliver Wolff
 */
@ToString
@EqualsAndHashCode
public class JwksAwareTokenParser implements JWTParser {

    private static final CuiLogger LOGGER = new CuiLogger(JwksAwareTokenParser.class);

    @Delegate
    private final JWTParser tokenParser;

    @Getter
    private final String jwksIssuer;

    public JwksAwareTokenParser(@NonNull String jwksEndpoint, @NonNull Integer jwksRefreshIntervall, @NonNull String jwksIssuer) {
        this.jwksIssuer = jwksIssuer;
        LOGGER.info(LogMessages.CONFIGURED_JWKS.format(jwksEndpoint, jwksRefreshIntervall, jwksIssuer));
        JWTAuthContextInfo contextInfo = new JWTAuthContextInfo();
        contextInfo.setPublicKeyLocation(jwksEndpoint);
        contextInfo.setJwksRefreshInterval(jwksRefreshIntervall);
        contextInfo.setIssuedBy(jwksIssuer);
        LOGGER.debug("Successfully configured JWTAuthContextInfo: %s", contextInfo);
        tokenParser = new DefaultJWTParser(contextInfo);
    }


}