package moofarm.ssuckssuck.domain.oauth.presentation.dto.response;

import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.domain.oauth.domain.vo.OauthMemberInfoVO;

public record OauthLoginResponse(
        String email,
        String name,
        OauthServerType oauthServerType,
        Boolean isFirst
) {
    public OauthLoginResponse (OauthMemberInfoVO oauthMemberInfoVO, Boolean status) {
        this(oauthMemberInfoVO.email(), oauthMemberInfoVO.name(), oauthMemberInfoVO.oauthServerType(), status);
    }
}