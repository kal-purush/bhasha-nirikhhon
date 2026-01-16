package com.petid.auth.test;

import com.petid.auth.jwt.TokenProvider;
import com.petid.domain.member.manager.MemberManager;
import com.petid.domain.member.model.Member;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth/test")
@RequiredArgsConstructor
public class TestController {

    private final TokenProvider tokenProvider;
    private final MemberManager memberManager;

    @GetMapping("/token")
    public String getToken(
            @RequestParam("memberId") long memberId
    ) {
        Member member = memberManager.get(memberId);

        return "Bearer " + tokenProvider.getAccessToken(member);
    }

    @GetMapping("/refreshToken")
    public String getRefreshToken(
            @RequestParam("memberId") long memberId
    ) {
        Member member = memberManager.get(memberId);

        return "Bearer " + tokenProvider.getRefreshToken(member);
    }
}