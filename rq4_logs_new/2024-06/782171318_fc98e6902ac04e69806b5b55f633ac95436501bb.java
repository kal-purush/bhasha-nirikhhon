package com.artfriendly.artfriendly.domain.term.controller;

import com.artfriendly.artfriendly.domain.term.dto.MemberTermReqDto;
import com.artfriendly.artfriendly.domain.term.service.MemberTermService;
import com.artfriendly.artfriendly.domain.term.service.TermService;
import com.artfriendly.artfriendly.global.api.RspTemplate;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/terms")
@RequiredArgsConstructor
public class MemberTermController {
    private final MemberTermService memberTermService;
    private final TermService termService;

    @PostMapping
    public RspTemplate<Void> createMemberTerms(@AuthenticationPrincipal long memberId, @RequestBody MemberTermReqDto memberTermReqDto) {
        memberTermService.createMemberTerm(memberId, memberTermReqDto);

        return new RspTemplate<>(HttpStatus.OK, "약관 동의 생성");
    }

    @PostMapping("/init")
    public RspTemplate<Void> initMemberTerms(@AuthenticationPrincipal long memberId) {
        termService.initTerms();

        return new RspTemplate<>(HttpStatus.OK, "약관 데이터 초기화");
    }
}