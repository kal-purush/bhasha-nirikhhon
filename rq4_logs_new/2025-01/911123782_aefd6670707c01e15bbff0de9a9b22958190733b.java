package com.roomfit.be.auth.application;

import com.roomfit.be.auth.application.exception.VerificationCodeNotFoundException;
import com.roomfit.be.auth.domain.VerificationCode;
import com.roomfit.be.auth.infrastructure.VerificationRepository;
import com.roomfit.be.email.application.EmailSendEvent;
import com.roomfit.be.global.event.EventPublisher;
import com.roomfit.be.global.util.RandomCodeGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class VerificationServiceImpl implements VerificationService {
    private static final int CODE_GENERATION_COUNT = 5;
    private final VerificationRepository verificationRepository;
    private final RandomCodeGenerator codeGenerator;
    private final EventPublisher eventPublisher;

    @Override
    @Async("taskExecutor")
    public void generateVerificationCode(String authToken, String email) {
        String code = codeGenerator.generateSecureRandomCode(CODE_GENERATION_COUNT);
        VerificationCode verificationCode = VerificationCode.of(authToken, code);
        
        verificationRepository.save(verificationCode)
                .doOnTerminate(() -> sendVerificationEmail(email, code))
                .subscribe();
    }

    @Override
    public boolean verifyVerificationCode(String authToken, String code) {
        VerificationCode verificationCode = verificationRepository.findByUUid(authToken)
                .orElseThrow(VerificationCodeNotFoundException::new);

        if (!verificationCode.isCodeValid(code)) {
            return false;
        }

        verificationCode.markEmailVerified();
        saveVerificationStatus(verificationCode);
        return true;
    }

    @Override
    public boolean getStatus(String authToken) {
        return verificationRepository.findStatusByAuthToken(authToken)
                .orElseThrow();
    }

    @Async("taskExecutor")
    protected void saveVerificationStatus(VerificationCode verificationCode) {
        verificationRepository.update(verificationCode).subscribe();
    }

    //retriable 하게
    private void sendVerificationEmail(String email, String code) {
        String subject = "Verification Code";
        String body = "Here is your verification code: " + code;
        eventPublisher.publish(EmailSendEvent.of(this, email, subject, body));
    }
}