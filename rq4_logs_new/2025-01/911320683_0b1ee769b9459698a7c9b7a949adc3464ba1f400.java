package com.sofkau.bank.services;

import com.sofkau.bank.entities.BlacklistedToken;
import com.sofkau.bank.repositories.BlacklistedTokenRepository;
import com.sofkau.bank.services.tokens.BlacklistedTokenServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class BlacklistedTokenServiceTest {
    @Mock
    private BlacklistedTokenRepository blacklistedTokenRepository;

    @InjectMocks
    private BlacklistedTokenServiceImpl blacklistedTokenService;

    @Test
    void whenIsBlacklistedReturnBoolean() {
        when(blacklistedTokenRepository.existsByToken(anyString()))
                .thenReturn(false);

        boolean result = assertDoesNotThrow(() -> blacklistedTokenService.isBlacklisted("tok3n"));

        assertFalse(result);
    }

    @Test
    void whenAddToBlacklistAndIsNotBlacklistedThenSaveAndReturnNothing() {
        when(blacklistedTokenRepository.existsByToken(anyString()))
                .thenReturn(false);

        assertDoesNotThrow(() -> blacklistedTokenService.addToBlacklist("tok3n"));

        verify(blacklistedTokenRepository, times(1))
                .save(any(BlacklistedToken.class));
    }

    @Test
    void whenAddToBlacklistAndIsBlacklistedThenDoNotSaveAndReturnNothing() {
        when(blacklistedTokenRepository.existsByToken(anyString()))
                .thenReturn(true);

        assertDoesNotThrow(() -> blacklistedTokenService.addToBlacklist("tok3n"));

        verify(blacklistedTokenRepository, never())
                .save(any());
    }
}