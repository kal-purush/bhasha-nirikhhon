package adhd.diary.auth.controller;

import adhd.diary.auth.dto.response.SocialLoginResponse;
import adhd.diary.auth.service.GoogleService;
import adhd.diary.auth.service.KakaoService;
import adhd.diary.response.ApiResponse;
import adhd.diary.response.ResponseCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/login")
public class LoginController {

    private final KakaoService kakaoService;
    private final GoogleService googleService;

    public LoginController(KakaoService kakaoService, GoogleService googleService) {
        this.kakaoService = kakaoService;
        this.googleService = googleService;
    }

    @PostMapping("/kakao")
    public ApiResponse<SocialLoginResponse> kakaoLogin(@RequestParam String code) throws JsonProcessingException {
        String accessToken = kakaoService.getKakaoAccessToken(code);
        SocialLoginResponse socialLoginResponse = kakaoService.kakaoLogin(accessToken);
        return ApiResponse.success(ResponseCode.MEMBER_LOGIN_SUCCESS, socialLoginResponse);
    }

    @PostMapping("/google")
    public ApiResponse<SocialLoginResponse> googleLogin(@RequestParam String code) {
        String accessToken = googleService.getGoogleAccessToken(code);
        SocialLoginResponse socialLoginResponse = googleService.googleLogin(accessToken);
        return ApiResponse.success(ResponseCode.MEMBER_LOGIN_SUCCESS, socialLoginResponse);
    }
}