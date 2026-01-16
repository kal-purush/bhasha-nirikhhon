package moofarm.ssuckssuck.global.oauth.google.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import moofarm.ssuckssuck.domain.oauth.domain.OauthMember;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;

import static moofarm.ssuckssuck.domain.oauth.domain.OauthServerType.GOOGLE;

@JsonNaming(value = SnakeCaseStrategy.class)
public record GoogleMemberResponse(
        String id,
        String email,
        Boolean verifiedEmail,
        String name,
        String givenName,
        String familyName,
        String picture,
        String locale
) {
    public OauthMember toDomain() {
        return OauthMember.builder()
                .name(name)
                .email(email)
                .oauthServerType(GOOGLE)
                .build();
    }
}