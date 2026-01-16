package nl.optifit.backendservice.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.optifit.backendservice.model.ExerciseType;
import nl.optifit.backendservice.model.Leaderboard;
import nl.optifit.backendservice.model.Session;
import nl.optifit.backendservice.repository.AccountRepository;
import nl.optifit.backendservice.repository.LeaderboardRepository;
import nl.optifit.backendservice.repository.SessionRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@Component
public class SessionScheduler {

    private final SessionRepository sessionRepository;
    private final AccountRepository accountRepository;
    private final LeaderboardRepository leaderboardRepository;

    @Scheduled(cron = "0 0 10,13,15 ? * MON-FRI", zone = "Europe/Amsterdam")
    public void createDailySessions() {
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        LocalTime currentTime = now.toLocalTime();

        if (isScheduledTime(currentTime)) {
            createSessionsForAllAccounts(now);
        }
    }

    private boolean isScheduledTime(LocalTime currentTime) {
        return currentTime.truncatedTo(ChronoUnit.MINUTES).equals(LocalTime.of(10, 0)) ||
                currentTime.truncatedTo(ChronoUnit.MINUTES).equals(LocalTime.of(13, 0)) ||
                currentTime.truncatedTo(ChronoUnit.MINUTES).equals(LocalTime.of(15, 0));
    }

    private void createSessionsForAllAccounts(LocalDateTime sessionTime) {
        log.info("Creating new sessions for all accounts at '{}'", sessionTime);
        accountRepository.findAll().forEach(account -> {
            Session session = Session.builder()
                    .account(account)
                    .sessionStart(sessionTime)
                    .exerciseType(determineExerciseType(sessionTime))
                    .build();

            sessionRepository.save(session);
            Leaderboard leaderboard = account.getLeaderboard();
            leaderboard.setCompletionRate(calculateCompletionRate(account.getId()));
            leaderboardRepository.save(leaderboard);
        });
    }

    private ExerciseType determineExerciseType(LocalDateTime time) {
        LocalTime localTime = time.toLocalTime();
        if (localTime.equals(LocalTime.of(10, 0))) {
            return ExerciseType.HIP;
        } else if (localTime.equals(LocalTime.of(13, 30))) {
            return ExerciseType.SHOULDER;
        } else {
            return ExerciseType.BACK;
        }
    }

    private double calculateCompletionRate(String accountId) {
        List<Session> sessionsForAccount = sessionRepository.findAllByAccountId(accountId);

        long totalSessions = sessionsForAccount.size();
        long completedSessions = sessionsForAccount.stream()
                .map(Session::getSessionExecutionTime)
                .filter(Objects::nonNull)
                .count();

        return totalSessions > 0
                ? (double) completedSessions / totalSessions * 100
                : 0;
    }
}