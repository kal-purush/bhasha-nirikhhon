package moofarm.ssuckssuck.global.oauth.naver.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import moofarm.ssuckssuck.domain.oauth.domain.OauthMember;

import static moofarm.ssuckssuck.domain.oauth.domain.OauthServerType.NAVER;

@JsonNaming(SnakeCaseStrategy.class)
public record NaverMemberResponse (
    String resultcode,
    String message,
    Response response
){
    public OauthMember toDomain() {
        return OauthMember.builder()
                .name(response.name)
                .email(response.email)
                .oauthServerType(NAVER)
                .build();


    }
    @JsonNaming(SnakeCaseStrategy.class)
    public record Response(
        String id,
        String nickname,
        String name,
        String email,
        String gender,
        String age,
        String birthday,
        String profileImage,
        String birthyear,
        String mobile


    ){

    }
}