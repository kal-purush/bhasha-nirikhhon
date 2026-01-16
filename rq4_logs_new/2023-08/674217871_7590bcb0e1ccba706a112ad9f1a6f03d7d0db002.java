package api.wantedpreonboardingbackend.domain.member.controller;

import api.wantedpreonboardingbackend.domain.member.entity.Member;
import api.wantedpreonboardingbackend.domain.member.service.MemberService;
import api.wantedpreonboardingbackend.domain.member.dto.request.JoinRequest;
import api.wantedpreonboardingbackend.global.base.ResponseForm;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/api/member", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
public class MemberController {
    private final MemberService memberService;

    @PostMapping("/")
    public ResponseEntity<ResponseForm<Member>> join(@Valid @RequestBody JoinRequest joinRequest) {
        Member member = memberService.join(joinRequest.getEmail(), joinRequest.getPassword());
        return ResponseEntity.ok(ResponseForm.of("S-001", "회원 가입 성공", member));
    }
}