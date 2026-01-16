package com.hanyahunya.gitRemind.unit.contribution.service;

import com.hanyahunya.gitRemind.contribution.dto.AlarmRequestDto;
import com.hanyahunya.gitRemind.contribution.dto.AlarmResponseDto;
import com.hanyahunya.gitRemind.contribution.dto.CommittedResponseDto;
import com.hanyahunya.gitRemind.contribution.dto.GitUsernameRequestDto;
import com.hanyahunya.gitRemind.contribution.entity.Contribution;
import com.hanyahunya.gitRemind.contribution.repository.ContributionRepository;
import com.hanyahunya.gitRemind.contribution.service.ContributionService;
import com.hanyahunya.gitRemind.contribution.service.ContributionServiceImpl;
import com.hanyahunya.gitRemind.contribution.util.AlarmTimeBitConverter;
import com.hanyahunya.gitRemind.infrastructure.github.GithubUserValidator;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.util.ResponseDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.*;

class ContributionServiceImplTest {
    private ContributionService contributionService;
    private ContributionRepository contributionRepository;

    @BeforeEach
    void setUp() {
        contributionRepository = Mockito.mock(ContributionRepository.class);
        contributionService = new ContributionServiceImpl(contributionRepository);
    }

    @Test
    void saveOrUpdateGitUsername() {
        // given
        String uuid = UUID.randomUUID().toString();
        GitUsernameRequestDto request = new GitUsernameRequestDto();
        request.setMemberId(uuid);
        request.setGitUsername("hanyahunya");

//        when(GithubUserValidator.isValid("gitUsername")).thenReturn(true);
        when(contributionRepository.updateContribution(argThat(contribution ->
                contribution.getMemberId().equals(uuid) &&
                contribution.getGitUsername().equals("hanyahunya")))
        ).thenReturn(true);

        // when
        ResponseDto<Void> response = contributionService.saveOrUpdateGitUsername(request);

        // then
        assertTrue(response.isSuccess());
        assertEquals("gitユーザー名設定成功", response.getMessage());

        verify(contributionRepository).updateContribution(any(Contribution.class));
    }
    @Test
    @DisplayName("<saveOrUpDateGitUsername>")
    void saveOrUpdateGitUsernameFailsWhenGitUsernameInvalid() {
        // given
        String uuid = UUID.randomUUID().toString();
        GitUsernameRequestDto request = new GitUsernameRequestDto();
        request.setMemberId(uuid);
        request.setGitUsername("InvalidGitUser_hanyahunya");

        // when
        ResponseDto<Void> response = contributionService.saveOrUpdateGitUsername(request);

        // then
        assertFalse(response.isSuccess());
        assertEquals("check git username", response.getMessage());
    }

    @Test
    void getAlarm() {
        // given
        String memberId = UUID.randomUUID().toString();
        // 3670016 == [20, 21, 22] to bit
        Contribution dbContribution = Contribution.builder().alarmBit(3670016).build();
        when(contributionRepository.getContributionByMemberId(eq(memberId))).thenReturn(Optional.of(dbContribution));

        // when
        ResponseDto<AlarmResponseDto> response = contributionService.getAlarm(memberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals("Alarm読み込み成功", response.getMessage());
        Set<Integer> hours = new HashSet<>();
        hours.add(20);
        hours.add(21);
        hours.add(22);
        assertEquals(hours, response.getData().getAlarmHours());
        verify(contributionRepository).getContributionByMemberId(eq(memberId));
    }

    @Test
    void setAlarm() {
        // given
        String uuid = UUID.randomUUID().toString();
        AlarmRequestDto request = new AlarmRequestDto();
        request.setMid(uuid);
        List<Integer> list = List.of(20, 21, 22);
        request.setAlarmHours(list);
        when(contributionRepository.updateContribution(argThat(contribution ->
                                contribution.getMemberId().equals(uuid) &&
                                contribution.getAlarmBit() == AlarmTimeBitConverter.hourToBit(list)
                ))).thenReturn(true);

        // when
        ResponseDto<Void> response = contributionService.setAlarm(request);

        // then
        assertTrue(response.isSuccess());
        assertEquals("Alarm設定成功", response.getMessage());
        verify(contributionRepository).updateContribution(any(Contribution.class));
    }

    @Test
    void getCommitStatus() {
        // given
        String memberId = UUID.randomUUID().toString();
        Contribution dbContribution = Contribution.builder()
                .email("test@example.com")
                .gitUsername("hanyahunya")
                .alarmBit(AlarmTimeBitConverter.hourToBit(List.of(20, 21)))
                .committed(true)
                .build();
        when(contributionRepository.getContributionByMemberId(eq(memberId))).thenReturn(Optional.of(dbContribution));

        // when
        ResponseDto<CommittedResponseDto> response = contributionService.getCommitStatus(memberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals("commit読み込み成功", response.getMessage());
        assertTrue(response.getData().isCommitted());
    }
}