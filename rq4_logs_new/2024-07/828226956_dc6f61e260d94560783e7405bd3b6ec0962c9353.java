package adhd.diary.auth.controller;

import adhd.diary.auth.dto.response.MemberKakaoLoginResponse;
import adhd.diary.auth.service.KakaoService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KakaoController {

    private final KakaoService authService;

    public KakaoController(KakaoService authService) {
        this.authService = authService;
    }

    @PostMapping("/login/oauth2/callback/kakao")
    public ResponseEntity<MemberKakaoLoginResponse> kakaoLogin(@RequestParam String code) throws JsonProcessingException {
        String accessToken = authService.getKakaoAccessToken(code);
        MemberKakaoLoginResponse memberKakaoLoginResponse = authService.kakaoLogin(accessToken);
        return ResponseEntity.ok(memberKakaoLoginResponse);
    }
}