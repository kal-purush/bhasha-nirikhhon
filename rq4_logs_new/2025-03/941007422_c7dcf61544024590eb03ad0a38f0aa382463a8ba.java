package com.example.auction.OAuth.Controller;

import com.example.auction.Global.CommonResponseBody;
import com.example.auction.OAuth.Dto.KakaoTokenDto;
import com.example.auction.OAuth.Dto.OAuthDto;
import com.example.auction.OAuth.Service.OAuthService;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@Slf4j
@RestController
@AllArgsConstructor
public class OAuthController {

    private OAuthService oAuthService;

    @ResponseBody
    @GetMapping("/auth/kakao/LoginHandler")
    public ResponseEntity<CommonResponseBody<KakaoTokenDto>> kakaoCallback(@RequestParam String code) {
        return ResponseEntity.ok().body(new CommonResponseBody<>("카카오 토큰",oAuthService.getKakaoAccessToken(code)));
    }
}