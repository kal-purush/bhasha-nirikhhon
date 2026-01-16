package com.jia.study_tracker;

import com.jia.study_tracker.domain.Summary;
import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.domain.User;
import com.jia.study_tracker.repository.SummaryRepository;
import com.jia.study_tracker.repository.UserRepository;
import com.jia.study_tracker.service.SlackNotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
@RequiredArgsConstructor
public class SlackMessageTestRunner implements CommandLineRunner {

    private final UserRepository userRepository;
    private final SummaryRepository summaryRepository;
    private final SlackNotificationService slackNotificationService;

    @Override
    public void run(String... args) {
        String slackUserId = "U08NPGDGQ7P";
        String slackUsername = "tester";

        // 유저가 없으면 생성
        User user = userRepository.findById(slackUserId)
                .orElseGet(() -> userRepository.save(new User(slackUserId, slackUsername)));

        // summary가 없으면 더미 summary 생성
        if (summaryRepository.findAll().isEmpty()) {
            Summary dummy = new Summary(
                    LocalDate.now(),
                    "이것은 테스트 요약입니다. 실제 데이터는 아님.",
                    "계속해서 좋은 학습 이어가세요! 👍",
                    true,
                    null,
                    user,
                    SummaryType.DAILY
            );
            summaryRepository.save(dummy);
            System.out.println("✔️ 더미 Summary 생성 완료");
        }

        Summary summary = summaryRepository.findAll().get(0);
        slackNotificationService.sendSummaryToUser(user, summary);
        System.out.println("🚀 슬랙 메시지 전송 시도 완료");
    }
}