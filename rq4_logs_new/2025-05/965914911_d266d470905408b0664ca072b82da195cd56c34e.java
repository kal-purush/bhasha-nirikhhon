package com.hanyahunya.gitRemind.token.service;

import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class SecurityAlertEmailServiceIT {
    @Autowired
    private SecurityAlertEmailService securityAlertEmailService;
    @Autowired
    private MemberRepository memberRepository;
    @Autowired
    private EncodeService encodeService;
    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    private final String krMemberId = UUID.randomUUID().toString();
    private final String jpMemberId = UUID.randomUUID().toString();
    private final String usMemberId = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        jdbcTemplate = new JdbcTemplate(dataSource);
        Member krMember = Member.builder()
                .memberId(krMemberId)
                .loginId("krId")
                .password(encodeService.encode("password"))
                .email("gkalsaka0103@gmail.com")
                .country("KR")
                .build();
        // add kr member
        memberRepository.saveMember(krMember);

        Member jpMember = Member.builder()
                .memberId(jpMemberId)
                .loginId("jpId")
                .password(encodeService.encode("password"))
                .email("gkalsaka0103@gmail.com")
                .country("JP")
                .build();
        // add kr member
        memberRepository.saveMember(jpMember);

        Member usMember = Member.builder()
                .memberId(usMemberId)
                .loginId("usId")
                .password(encodeService.encode("password"))
                .email("java020103@gmail.com")
                .country("US")
                .build();
        // add kr member
        memberRepository.saveMember(usMember);
    }

    @Test
    void sendCookieHijackingAlertToKr() {
        // when
    }
}