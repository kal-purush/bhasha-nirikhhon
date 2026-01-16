package com.umc.accountbook.controller;

import com.umc.accountbook.service.KakaoOAuthService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
@RequestMapping("/oauth")
public class KakaoOAuthController {
    private final KakaoOAuthService kakaoOAuthService;
    /**
     * 카카오 callback
     * [GET] /oauth/kakao/callback
     */
    @ResponseBody
    @GetMapping("/kakao")
    public void kakaoCallback(@RequestParam String code) throws Exception{
        //System.out.println(code);
        kakaoOAuthService.getKakaoAccessToken(code);
    }
}