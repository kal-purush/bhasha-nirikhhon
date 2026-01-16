package com.hanyahunya.gitRemind.integration.member.service;

import com.hanyahunya.gitRemind.member.dto.*;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.member.service.MemberService;
import com.hanyahunya.gitRemind.token.service.TokenService;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.cookieHeader.SetResultDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class MemberServiceImplIT {
    @Autowired
    private MemberService memberService;
    @Autowired
    private MemberRepository memberRepository;
    @Autowired
    private TokenService tokenService;
    @Autowired
    private EncodeService encodeService;

    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    private final String defaultMemberId = UUID.randomUUID().toString();

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

    @Test
    void join() {
        // given
        JoinRequestDto requestDto = new JoinRequestDto();
        requestDto.setLoginId("test");
        requestDto.setPassword("rawPassword");
        requestDto.setEmail("test@example.com");
        requestDto.setCountry("KR");

        // when
        ResponseDto<Void> response = memberService.join(requestDto);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT member_id, login_id, password, email, country FROM member WHERE login_id = ?";
        Map<String, Object> rows = jdbcTemplate.queryForMap(sql, "test");
        assertNotNull(rows.get("member_id"));
        assertEquals("test", rows.get("login_id"));
        assertTrue(encodeService.matches("rawPassword", (String)rows.get("password")));
        assertEquals("test@example.com", rows.get("email"));
        assertEquals("KR", rows.get("country"));
    }

    @Test
    void login() {
        // given
        LoginRequestDto requestDto = new LoginRequestDto();
        requestDto.setLoginId("id");
        requestDto.setPassword("password");

        // when
        SetResultDto response = memberService.login(requestDto);

        // then
        assertTrue(response.isSuccess());
        assertNotNull(response.getAccessToken());
        assertNotNull(response.getRefreshToken());
    }
    @Test
    @DisplayName("<login>Idに該当するユーザーがない場合の失敗")
    void loginFailsWhenMemberNotFound() {
        // given
        LoginRequestDto requestDto = new LoginRequestDto();
        requestDto.setLoginId("wrongId");
        requestDto.setPassword("password");

        // when
        SetResultDto response = memberService.login(requestDto);

        // then
        assertFalse(response.isSuccess());
    }
    @Test
    @DisplayName("<login>パスワード不一致からの失敗")
    void loginFailsWhenPasswordIncorrect() {
        // given
        LoginRequestDto requestDto = new LoginRequestDto();
        requestDto.setLoginId("id");
        requestDto.setPassword("wrongPassword");

        // when
        SetResultDto response = memberService.login(requestDto);

        // then
        assertFalse(response.isSuccess());
    }

    @Test
    void getInfo() {
        // given
        String memberId = defaultMemberId;

        // when
        ResponseDto<MemberInfoResponseDto> response = memberService.getInfo(memberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals("default@example.com", response.getData().getEmail());
        assertEquals("KR", response.getData().getCountry());
    }
    @Test
    @DisplayName("<getInfo>Idに該当するユーザーがない場合の失敗")
    void getInfoFailsWhenMemberNotFound() {
        // given
        String wrongMemberId = UUID.randomUUID().toString();

        // when
        ResponseDto<MemberInfoResponseDto> response = memberService.getInfo(wrongMemberId);

        // then
        assertFalse(response.isSuccess());
    }

    @Test
    void deleteMember() {
        // given
        DeleteMemberRequestDto request = new DeleteMemberRequestDto();
        request.setMemberId(defaultMemberId);
        request.setLoginId("id");
        request.setPassword("password");

        // when
        SetResultDto response = memberService.deleteMember(request);

        // then
        assertTrue(response.isSuccess());
        assertTrue(response.isDeleteAccessToken());
        assertTrue(response.isDeleteRefreshToken());

        String sql = "SELECT COUNT(*) FROM member WHERE login_id = ?";
        int rows = jdbcTemplate.queryForObject(sql, Integer.class, "id");
        assertEquals(0, rows);
    }
    @Test
    @DisplayName("<deleteMember>パスワード不一致からの失敗")
    void deleteMemberFailsWhenPasswordIncorrect() {
        // given
        DeleteMemberRequestDto request = new DeleteMemberRequestDto();
        request.setMemberId(defaultMemberId);
        request.setLoginId("id");
        request.setPassword("wrongPassword");

        // when
        SetResultDto response = memberService.deleteMember(request);

        // then
        assertFalse(response.isSuccess());

        String sql = "SELECT COUNT(*) FROM member WHERE login_id = ?";
        int rows = jdbcTemplate.queryForObject(sql, Integer.class, "id");
        assertEquals(1, rows);
    }

    @Test
    void updateMember() {
        // given
        UpdateMemberRequestDto request = new UpdateMemberRequestDto();
        request.setMemberId(defaultMemberId);
        request.setLoginId("id");
        request.setPassword("password");
        request.setEmail("new@example.com");
        request.setCountry("JP");

        // when
        ResponseDto<Void> response = memberService.updateMember(request);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT email, country FROM member WHERE member_id = ?";
        Map<String, Object> rows = jdbcTemplate.queryForMap(sql, defaultMemberId);
        assertEquals("new@example.com", rows.get("email"));
        assertEquals("JP", rows.get("country"));
    }
}