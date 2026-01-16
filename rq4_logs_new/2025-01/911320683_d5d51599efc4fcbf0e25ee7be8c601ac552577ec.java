package com.sofkau.bank.services.tokens.auth;

import com.sofkau.bank.utils.records.Session;
import com.sofkau.bank.entities.Client;
import com.sofkau.bank.services.tokens.BlacklistedTokenService;
import com.sofkau.bank.utils.jwt.JwtInterpreter;
import org.springframework.stereotype.Service;

@Service
public class JwtTokenAuthServiceImpl implements JwtTokenAuthService {
    private final JwtInterpreter jwtInterpreter;
    private final BlacklistedTokenService blacklistedTokenService;

    public JwtTokenAuthServiceImpl(JwtInterpreter jwtInterpreter, BlacklistedTokenService blacklistedTokenService) {
        this.jwtInterpreter = jwtInterpreter;
        this.blacklistedTokenService = blacklistedTokenService;
    }

    @Override
    public Session generateAccessTokenFrom(Client client) {
        String token = jwtInterpreter
                .generateAccessToken(client.getUsername());

        return new Session(token, jwtInterpreter.getExpirationTime());
    }

    @Override
    public void destroyAccessToken(String token) {
        blacklistedTokenService.addToBlacklist(token);
    }
}