package com.roomfit.be.auth.infrastructure;

import com.roomfit.be.auth.domain.VerificationCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Repository
@Slf4j
public class VerificationRepositoryImpl implements VerificationRepository{
    private final static String  VERIFICATION_CODE_HASH_NAME = "verification_code";
    private final ReactiveRedisTemplate<String, VerificationCode> reactiveRedisTemplate;
    private final ReactiveHashOperations<String, String, VerificationCode> hashOperations;


    @Autowired
    public VerificationRepositoryImpl(@Qualifier("verificationCodeRedisTemplate") ReactiveRedisTemplate<String, VerificationCode>reactiveRedisTemplate) {
        this.reactiveRedisTemplate =  reactiveRedisTemplate;
        this.hashOperations = reactiveRedisTemplate.opsForHash();
    }

    @Override
    public Optional<VerificationCode> findByUUid(String uuid) {
        VerificationCode verificationCode = (VerificationCode) reactiveRedisTemplate.opsForHash()
                .get(VERIFICATION_CODE_HASH_NAME, uuid)
                .block();
        return Optional.ofNullable(verificationCode);
    }

    @Override
    public Optional<Boolean> findStatusByAuthToken(String uuid) {
        VerificationCode verificationCode = (VerificationCode) reactiveRedisTemplate.opsForHash()
                .get(VERIFICATION_CODE_HASH_NAME, uuid)
                .block();
        return Optional.ofNullable(verificationCode)
                .map(VerificationCode::getIsEmailVerified);
    } 
    @Override
    public Mono<VerificationCode> save(VerificationCode verificationCode) {
        return hashOperations.put(VERIFICATION_CODE_HASH_NAME, verificationCode.getUuid(), verificationCode)
                .doOnTerminate(() -> log.info("Saved verification code for UUID: " + verificationCode.getUuid()))
                .doOnError(error -> log.error("Error saving verification code", error))
                .thenReturn(verificationCode);
    }

    @Override
    public Mono<VerificationCode> update(VerificationCode verificationCode) {
        return hashOperations.put(VERIFICATION_CODE_HASH_NAME, verificationCode.getUuid(), verificationCode)
                .thenReturn(verificationCode);
    }
}