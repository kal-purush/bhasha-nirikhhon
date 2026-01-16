package com.hanyahunya.gitRemind.unit.token.service;

import com.hanyahunya.gitRemind.contribution.repository.ContributionRepository;
import com.hanyahunya.gitRemind.token.dto.JwtTokenPairResponseDto;
import com.hanyahunya.gitRemind.token.entity.Token;
import com.hanyahunya.gitRemind.token.repository.MemberTokenRepository;
import com.hanyahunya.gitRemind.token.repository.TokenRepository;
import com.hanyahunya.gitRemind.token.service.AccessTokenService;
import com.hanyahunya.gitRemind.token.service.RefreshTokenService;
import com.hanyahunya.gitRemind.token.service.SecurityAlertEmailService;
import com.hanyahunya.gitRemind.token.service.TokenServiceImpl;
import com.hanyahunya.gitRemind.util.ResponseDto;
import com.hanyahunya.gitRemind.util.cookieHeader.SetResultDto;
import com.hanyahunya.gitRemind.util.service.EncodeService;
import io.jsonwebtoken.Claims;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TokenServiceImplTest {
    @InjectMocks
    private TokenServiceImpl tokenService;
    @Mock
    private TokenRepository tokenRepository;
    @Mock
    private MemberTokenRepository memberTokenRepository;
    @Mock
    private ContributionRepository contributionRepository;
    @Mock
    private AccessTokenService accessTokenService;
    @Mock
    private RefreshTokenService refreshTokenService;
    @Mock
    private EncodeService encodeService;
    @Mock
    private SecurityAlertEmailService securityAlertEmailService;

    @Test
    void issueTokens() {
        // given
        String memberId = UUID.randomUUID().toString();
        when(accessTokenService.generateToken(eq(memberId), any(String.class))).thenReturn("access_token");
        when(refreshTokenService.generateToken(any(String.class))).thenReturn("refresh_token");
        when(encodeService.encode("access_token")).thenReturn("encoded_access_token");
        when(encodeService.encode("refresh_token")).thenReturn("encoded_refresh_token");
        when(tokenRepository.saveToken(argThat(token ->
                        token.getTokenId() != null &&
                        token.getAccessToken().equals("encoded_access_token") &&
                        token.getRefreshToken().equals("encoded_refresh_token")
        ))).thenReturn(true);
        when(memberTokenRepository.saveMemberToken(eq(memberId), any(String.class))).thenReturn(true);

        Claims mockAccessClaims = mock(Claims.class);
        Claims mockRefreshClaims = mock(Claims.class);
        Date accessExp = new Date(System.currentTimeMillis() + 1000 * 60 * 15); // +15 min
        Date refreshExp = new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 7); // +7 days
        when(mockAccessClaims.getExpiration()).thenReturn(accessExp);
        when(mockRefreshClaims.getExpiration()).thenReturn(refreshExp);
        when(accessTokenService.getClaims("access_token")).thenReturn(mockAccessClaims);
        when(refreshTokenService.getClaims("refresh_token")).thenReturn(mockRefreshClaims);

        //when
        ResponseDto<JwtTokenPairResponseDto> response = tokenService.issueTokens(memberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals("Token生成成功", response.getMessage());
        assertEquals("access_token", response.getData().getAccessToken());
        assertEquals("refresh_token", response.getData().getRefreshToken());
    }

    @Test // アクセストークンの更新、リフレッシュトークンは更新しない場合
    void refreshAccessToken() {
        //given
        String oldAccessToken = "old_access_token";
        String refreshToken = "refresh_token";
        /* refresh_token_claim_mock */
        Claims mockRefreshClaims = mock(Claims.class);
        String claimTokenId = UUID.randomUUID().toString();
        Date refreshExp = new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 5); // +5 days
        when(mockRefreshClaims.get("token_id", String.class)).thenReturn(claimTokenId);
        when(mockRefreshClaims.getExpiration()).thenReturn(refreshExp);
        when(refreshTokenService.getClaims("refresh_token")).thenReturn(mockRefreshClaims);

        Date oldAccessExp = new Date(System.currentTimeMillis() - 1000 * 60 * 5); // -5 min
        Token dbToken = Token.builder()
                .tokenId(claimTokenId)
                .accessToken("encoded_old_access_token")
                .refreshToken("encoded_refresh_token")
                .accessTokenExpiry(oldAccessExp)
                .build();
        when(tokenRepository.findByTokenId(claimTokenId)).thenReturn(Optional.of(dbToken));

        String memberId = UUID.randomUUID().toString();
        when(memberTokenRepository.findMemberIdByTokenId(claimTokenId)).thenReturn(memberId);

        when(accessTokenService.isTokenExpired(oldAccessExp)).thenReturn(true);

        when(encodeService.matches(oldAccessToken, "encoded_old_access_token")).thenReturn(true);
        when(encodeService.matches(refreshToken, "encoded_refresh_token")).thenReturn(true);

        when(accessTokenService.generateToken(memberId, claimTokenId)).thenReturn("new_access_token");
        when(encodeService.encode("new_access_token")).thenReturn("encoded_new_access_token");

        /* new_access_token_claim_mock */
        Claims mockAccessClaims = mock(Claims.class);
        Date accessExp = new Date(System.currentTimeMillis() + 1000 * 60 * 15); // +15 min
        when(mockAccessClaims.getExpiration()).thenReturn(accessExp);
        when(accessTokenService.getClaims("new_access_token")).thenReturn(mockAccessClaims);

        when(tokenRepository.updateToken(argThat(token ->
                        token.getTokenId().equals(claimTokenId) &&
                        token.getAccessToken().equals("encoded_new_access_token") &&
                        token.getAccessTokenExpiry() == accessExp
                ))).thenReturn(true);

        // when
        SetResultDto response = tokenService.refreshAccessToken(oldAccessToken, refreshToken);

        // then
        assertTrue(response.isSuccess());
        assertEquals("new_access_token", response.getAccessToken());
    }

    @Test
    void deleteTokenAtAllDevice() {
        // given
        String memberId = UUID.randomUUID().toString();
        when(memberTokenRepository.deleteAllByMemberId(memberId)).thenReturn(true);

        // when
        ResponseDto<Void> response = tokenService.deleteTokenAtAllDevice(memberId);

        // then
        assertTrue(response.isSuccess());
        assertEquals("すべてのデヴァイスからログアウト成功", response.getMessage());
    }

    @Test
    void deleteTokenCurrentDevice() {
        // given
        String tokenId = UUID.randomUUID().toString();
        when(tokenRepository.deleteByTokenId(tokenId)).thenReturn(true);

        // when
        SetResultDto response = tokenService.deleteTokenCurrentDevice(tokenId);

        // then
        assertTrue(response.isSuccess());
        assertTrue(response.isDeleteAccessToken());
        assertTrue(response.isDeleteRefreshToken());
    }

    @Test
    void deleteTokenOtherDevice() {
        // given
        String memberId = UUID.randomUUID().toString();
        String tokenId = UUID.randomUUID().toString();

        // when
        tokenService.deleteTokenOtherDevice(memberId, tokenId);

        //then
        verify(memberTokenRepository).deleteAllByMemberIdAndTokenIdNot(memberId, tokenId);
    }
}