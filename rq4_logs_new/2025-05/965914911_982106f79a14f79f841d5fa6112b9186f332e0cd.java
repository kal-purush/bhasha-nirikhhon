package com.hanyahunya.gitRemind.integration.member.service;

import com.hanyahunya.gitRemind.member.dto.ChangePwRequestDto;
import com.hanyahunya.gitRemind.member.dto.ResetPwRequestDto;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.member.service.PasswordService;
import com.hanyahunya.gitRemind.token.entity.Token;
import com.hanyahunya.gitRemind.token.service.TokenService;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class PasswordServiceImplIT {
    @Autowired
    private PasswordService passwordService;
    @Autowired
    private MemberRepository memberRepository;
    @Autowired
    private EncodeService encodeService;
    @Autowired
    private TokenService tokenService;
    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    private final String defaultMemberId = UUID.randomUUID().toString();
    private final String token1tokenId = UUID.randomUUID().toString();
    private final String token2tokenId = UUID.randomUUID().toString();

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
        // ログイン中のデバイス1
        String sqlToken1 = "INSERT INTO token  VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(sqlToken1,
                token1tokenId,
                "encoded_access_token",
                "encoded_refresh_token",
                new Timestamp(new Date().getTime()),
                new Timestamp(new Date().getTime())
        );
        String sqlMemberToken1 = "INSERT INTO member_token (member_id, token_id) VALUES (?, ?)";
        jdbcTemplate.update(sqlMemberToken1, defaultMemberId, token1tokenId);
        // ログイン中のデバイス2
        String sqlToken2 = "INSERT INTO token  VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(sqlToken2,
                token2tokenId,
                "encoded_access_token",
                "encoded_refresh_token",
                new Timestamp(new Date().getTime()),
                new Timestamp(new Date().getTime())
        );
        String sqlMemberToken2 = "INSERT INTO member_token (member_id, token_id) VALUES (?, ?)";
        jdbcTemplate.update(sqlMemberToken2, defaultMemberId, token2tokenId);
    }

    @Test
    void forgotPassword() {
        // given
        ResetPwRequestDto request = new ResetPwRequestDto();
        request.setEmail("default@example.com");
        request.setNewPassword("newPassword");

        // when
        ResponseDto<Void> response = passwordService.forgotPassword(request);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT password FROM member WHERE member_id = ?";
        String dbPassword = jdbcTemplate.queryForObject(sql, String.class, defaultMemberId);
        assertTrue(encodeService.matches("newPassword", dbPassword));
    }

    @Test
    void changePassword() {
        // given
        ChangePwRequestDto request = new ChangePwRequestDto();
        request.setTokenId(token1tokenId);
        request.setMemberId(defaultMemberId);
        request.setOldPassword("password");
        request.setNewPassword("newPassword");

        // when
        ResponseDto<Void> response = passwordService.changePassword(request);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT password FROM member WHERE member_id = ?";
        String dbPassword = jdbcTemplate.queryForObject(sql, String.class, defaultMemberId);
        assertTrue(encodeService.matches("newPassword", dbPassword));

        String sql1 = "SELECT COUNT(*) FROM token WHERE token_id = ?";
        Integer rows1 = jdbcTemplate.queryForObject(sql1, Integer.class, token1tokenId);
        assertEquals(1, rows1);

        String sql2 = "SELECT COUNT(*) FROM token WHERE token_id = ?";
        Integer rows2 = jdbcTemplate.queryForObject(sql2, Integer.class, token2tokenId);
        assertEquals(0, rows2);
    }
}