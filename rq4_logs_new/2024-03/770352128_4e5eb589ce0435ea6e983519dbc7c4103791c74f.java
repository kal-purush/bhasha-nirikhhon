package moofarm.ssuckssuck.domain.oauth.domain;

import jakarta.persistence.*;
import lombok.*;
import moofarm.ssuckssuck.domain.oauth.domain.vo.OauthMemberInfoVO;

import static jakarta.persistence.EnumType.STRING;
import static jakarta.persistence.GenerationType.IDENTITY;
import static lombok.AccessLevel.PROTECTED;

@Entity
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor(access = PROTECTED)
public class OauthMember {

    @Id
    @GeneratedValue(strategy = IDENTITY)
    private Long id;

    @Enumerated(STRING)
    private OauthServerType oauthServerType;

    private String name;
    private String email;

    public OauthMemberInfoVO getOauthMemberInfo() {
        return new OauthMemberInfoVO(
                email,
                name,
                oauthServerType
        );
    }
}