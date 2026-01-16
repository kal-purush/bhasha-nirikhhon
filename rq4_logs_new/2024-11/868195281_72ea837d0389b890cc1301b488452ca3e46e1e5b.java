package com.filegenie.backend.Services;

import com.filegenie.backend.DTO.HttpException;
import com.filegenie.backend.Entities.UserSession;
import com.filegenie.backend.Repositories.UserSessionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class UserSessionService {
    @Autowired
    private UserSessionRepository userSessionRepository;

    @Scheduled(cron = "0 0 * * * *")
    public void removeExpiredSessions() {
        userSessionRepository.deleteAllByExpiresAtBefore(LocalDateTime.now());
    }

    public boolean verifyToken(String token) throws HttpException {
        Optional<UserSession> userSession = userSessionRepository.findBySessionToken(token);

        if (userSession.isEmpty()) throw new HttpException(HttpStatus.NOT_FOUND, "Jeton introuvable");
        else return !userSession.get().getExpiresAt().isBefore(LocalDateTime.now());
    }
}