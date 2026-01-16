package adhd.diary.auth.controller;

import adhd.diary.auth.dto.response.TokenResponse;
import adhd.diary.auth.exception.token.TokenNotFoundException;
import adhd.diary.auth.jwt.JwtService;
import adhd.diary.response.ApiResponse;
import adhd.diary.response.ResponseCode;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AuthController {

    private final JwtService jwtService;

    public AuthController(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @PostMapping("/api/auth/token/refresh")
    public ApiResponse<?> refreshAccessToken(HttpServletResponse response, @RequestHeader("RefreshToken") String refreshToken) {
        try {
            TokenResponse tokenResponse = jwtService.refreshTokens(refreshToken);
            jwtService.sendAccessAndRefreshToken(response, tokenResponse.getAccessToken(), tokenResponse.getRefreshToken());

            return ApiResponse.success(ResponseCode.JWT_REFRESH_SUCCESS, tokenResponse);
        } catch (TokenNotFoundException e) {
            return ApiResponse.fail(ResponseCode.JWT_REFRESH_TOKEN_EXPIRED, e.getMessage());
        } catch (Exception e) {
            return ApiResponse.fail(ResponseCode.UNEXPECTED_ERROR, e.getMessage());
        }
    }
}