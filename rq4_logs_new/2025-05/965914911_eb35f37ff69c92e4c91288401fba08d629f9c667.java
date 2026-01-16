package com.hanyahunya.gitRemind.integration.contribution.service;

import com.hanyahunya.gitRemind.contribution.dto.AlarmRequestDto;
import com.hanyahunya.gitRemind.contribution.dto.AlarmResponseDto;
import com.hanyahunya.gitRemind.contribution.dto.CommittedResponseDto;
import com.hanyahunya.gitRemind.contribution.dto.GitUsernameRequestDto;
import com.hanyahunya.gitRemind.contribution.repository.ContributionRepository;
import com.hanyahunya.gitRemind.contribution.service.ContributionService;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class ContributionServiceImplIT {
    @Autowired
    private ContributionService contributionService;

    @Autowired
    private EncodeService encodeService;
    @Autowired
    private MemberRepository memberRepository;
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
        // 3670016 == [20, 21, 22] to bit
        String sql = "UPDATE member SET alarm_hour_bit = 3670016";
        jdbcTemplate.update(sql);
    }

    @Test
    void saveOrUpdateGitUsername() {
        // given
        GitUsernameRequestDto request = new GitUsernameRequestDto();
        request.setMemberId(defaultMemberId);
        request.setGitUsername("hanyahunya");

        // when
        ResponseDto<Void> response = contributionService.saveOrUpdateGitUsername(request);

        // then
        assertTrue(response.isSuccess());

        String sql = "SELECT git_username FROM member WHERE member_id = ?";
        String dbGitUsername = jdbcTemplate.queryForObject(sql, String.class, defaultMemberId);
        assertEquals("hanyahunya", dbGitUsername);
    }
    @Test
    @DisplayName("<saveOrUpDateGitUsername>Githubに該当するユーザーが無かった場合")
    void saveOrUpdateGitUsernameFailsWhenGitUsernameInvalid() {
        // given
        GitUsernameRequestDto request = new GitUsernameRequestDto();
        request.setMemberId(defaultMemberId);
        request.setGitUsername("InvalidGitUser_hanyahunya");

        // when
        ResponseDto<Void> response = contributionService.saveOrUpdateGitUsername(request);

        // then
        assertFalse(response.isSuccess());
    }

    @Test
    void getAlarm() {
        // when
        ResponseDto<AlarmResponseDto> response = contributionService.getAlarm(defaultMemberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals(Set.of(20, 21, 22), response.getData().getAlarmHours());
    }

    @Test
    void setAlarm() {
        // given
        AlarmRequestDto request = new AlarmRequestDto();
        request.setMid(defaultMemberId);
        request.setAlarmHours(List.of(13, 14, 15));

        // when
        ResponseDto<Void> response = contributionService.setAlarm(request);

        // then
        assertTrue(response.isSuccess());
        String sql = "SELECT alarm_hour_bit FROM member WHERE member_id = ?";
        int alarmHourBit = jdbcTemplate.queryForObject(sql, Integer.class, defaultMemberId);
        // 28672 = [13, 14, 15]
        assertEquals(28672, alarmHourBit);
    }

    @Test
    void getCommitStatus() {
        // when
        ResponseDto<CommittedResponseDto> response = contributionService.getCommitStatus(defaultMemberId);

        // then
        assertTrue(response.isSuccess());
        assertFalse(response.getData().isCommitted());
    }
}