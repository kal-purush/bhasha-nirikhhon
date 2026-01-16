package com.hanyahunya.gitRemind.contribution.service;

import com.hanyahunya.gitRemind.contribution.entity.Contribution;
import com.hanyahunya.gitRemind.infrastructure.email.SendEmailService;
import com.hanyahunya.gitRemind.member.entity.Member;
import com.hanyahunya.gitRemind.member.repository.MemberRepository;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class CommitReminderEmailService {
    private final SendEmailService sendEmailService;
    private final MemberRepository memberRepository;

    public void sendCommitRemindEmail(Contribution contribution) {
        Optional<Member> optionalMember = memberRepository.findMemberByEmail(contribution.getEmail());
        if (optionalMember.isEmpty()) {
            return;
        }
        switch (optionalMember.get().getCountry().toLowerCase()) {
            case "kr" :
                sendToKorea(contribution);
                break;
            case "jp" :
                sendToJapan(contribution);
                break;
            default:
                sendToUnitedStates(contribution);
                break;
        }

    }

    private void sendToKorea(Contribution contribution) {
        Map<String, Object> params = new HashMap<>();
        if (contribution.getGitUsername() != null) {
            params.put("username", contribution.getGitUsername());
        } else {
            params.put("username", "사용자");
        }
        String subject = contribution.getGitUsername() + "님 아직 커밋을 안하셨어요!";
        sendEmailService.sendEmail(contribution.getEmail(), subject, "kr/gitRemind", params);
    }

    private void sendToJapan(Contribution contribution) {
        Map<String, Object> params = new HashMap<>();
        if (contribution.getGitUsername() != null) {
            params.put("username", contribution.getGitUsername());
        } else {
            params.put("username", "ユーザー");
        }
        String subject = contribution.getGitUsername() + "さん、まだコミットしていません！";
        sendEmailService.sendEmail(contribution.getEmail(), subject, "jp/gitRemind", params);
    }

    private void sendToUnitedStates(Contribution contribution) {
        Map<String, Object> params = new HashMap<>();
        if (contribution.getGitUsername() != null) {
            params.put("username", contribution.getGitUsername());
        } else {
            params.put("username", "User");
        }
        String subject = contribution.getGitUsername() + ", you haven't committed yet!";
        sendEmailService.sendEmail(contribution.getEmail(), subject, "us/gitRemind", params);
    }
}