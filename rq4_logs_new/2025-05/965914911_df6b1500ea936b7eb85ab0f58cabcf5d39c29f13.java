package com.hanyahunya.gitRemind.integration.token.service;

import com.hanyahunya.gitRemind.contribution.repository.ContributionRepository;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.token.dto.JwtTokenPairResponseDto;
import com.hanyahunya.gitRemind.token.repository.MemberTokenRepository;
import com.hanyahunya.gitRemind.token.repository.TokenRepository;
import com.hanyahunya.gitRemind.token.service.AccessTokenService;
import com.hanyahunya.gitRemind.token.service.RefreshTokenService;
import com.hanyahunya.gitRemind.token.service.SecurityAlertEmailService;
import com.hanyahunya.gitRemind.token.service.TokenService;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/* テストの場合によって違うトークン有効期限が必要のため、System.setProperty()を使用。
 そのため、このテストクラス終了後、このSpringContextを廃棄 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@Transactional
class TokenServiceImplIT {
    @Autowired
    private TokenService tokenService;
    @Autowired
    private TokenRepository tokenRepository;
    @Autowired
    private MemberTokenRepository memberTokenRepository;
    @Autowired
    private ContributionRepository contributionRepository;
    @Autowired
    private MemberRepository memberRepository;
    @Autowired
    private AccessTokenService accessTokenService;
    @Autowired
    private RefreshTokenService refreshTokenService;
    @Autowired
    private EncodeService encodeService;
    @Autowired
    private SecurityAlertEmailService securityAlertEmailService;

    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;


    private final String defaultMemberId = UUID.randomUUID().toString();
    private final String defaultTokenId = UUID.randomUUID().toString();
    private String defaultAccessToken;
    private String defaultRefreshToken;


    @BeforeEach
    void setUp() {
        jdbcTemplate = new JdbcTemplate(dataSource);
        Member defaultMember = Member.builder()
                .memberId(defaultMemberId)
                .loginId("id")
                .password(encodeService.encode("password"))
                .email("default@example.com")
                .country("KR")
                .build();
        // add default member
        memberRepository.saveMember(defaultMember);
    }

    @AfterEach
    void tearDown() {
        System.setProperty("jwt.accessToken.expiration", "900000");
    }

    @Test
    void issueTokens() {
        // when
        ResponseDto<JwtTokenPairResponseDto> response = tokenService.issueTokens(defaultMemberId);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT token_id FROM member_token WHERE member_id = ?";
        String dbTokenId = jdbcTemplate.queryForObject(sql, String.class, defaultMemberId);
        assertNotNull(dbTokenId);

        String sql1 = "SELECT access_token, refresh_token FROM token WHERE token_id = ?";
        Map<String, Object> dbToken = jdbcTemplate.queryForMap(sql1, dbTokenId);

        assertTrue(encodeService.matches(response.getData().getAccessToken(), dbToken.get("access_token").toString()));
        assertTrue(encodeService.matches(response.getData().getRefreshToken(), dbToken.get("refresh_token").toString()));
    }

    @Test
    void refreshAccessToken() {
        // given
        System.setProperty("jwt.accessToken.expiration", "5000");

    }

    @Test
    void deleteTokenAtAllDevice() {
    }

    @Test
    void deleteTokenCurrentDevice() {
    }

    @Test
    void deleteTokenOtherDevice() {
    }
}