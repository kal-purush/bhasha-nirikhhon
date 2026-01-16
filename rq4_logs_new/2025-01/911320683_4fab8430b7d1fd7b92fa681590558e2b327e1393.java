package com.sofkau.bank.services.tokens;

import com.sofkau.bank.entities.BlacklistedToken;
import com.sofkau.bank.repositories.BlacklistedTokenRepository;
import org.springframework.stereotype.Service;

@Service
public class BlacklistedTokenServiceImpl implements BlacklistedTokenService {
    private final BlacklistedTokenRepository blacklistedTokenRepository;

    public BlacklistedTokenServiceImpl(BlacklistedTokenRepository blacklistedTokenRepository) {
        this.blacklistedTokenRepository = blacklistedTokenRepository;
    }

    @Override
    public boolean isBlacklisted(String token) {
        return blacklistedTokenRepository.existsByToken(token);
    }

    @Override
    public void addToBlacklist(String token) {
        blacklistedTokenRepository.save(new BlacklistedToken(token));
    }
}