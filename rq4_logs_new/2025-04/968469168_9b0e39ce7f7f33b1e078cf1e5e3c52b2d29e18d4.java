package com.doubleo.memberservice.domain.member.controller;

import com.doubleo.memberservice.domain.member.domain.Member;
import com.doubleo.memberservice.domain.member.dto.request.MemberCreateRequest;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Tag(name = "1-1. Member API", description = "회원 API")
@RestController
@RequiredArgsConstructor
@RequestMapping("/members")
public class MemberController {

    // 회원 가입
    @PostMapping("/create")
    public ResponseEntity<Member> memberJoin(@RequestBody MemberCreateRequest request) {
        return ResponseEntity.ok(new Member());
    }

    // 회원 개별 정보 조회
    @GetMapping("/{memberId}")
    public ResponseEntity<Member> memberGet() {
        return ResponseEntity.ok(new Member());
    }

    // 회원 전체 목록 조회
    //    @GetMapping("/")
    //    public ResponseEntity<Member> memberListGet() {
    //        return ResponseEntity.ok(new Member());
    //    }

    // 회원 정보 업데이트
    @PatchMapping("/{memberId}")
    public ResponseEntity<Member> memberUpdate() {
        return ResponseEntity.ok(new Member());
    }

    // 회원 탈퇴
    @DeleteMapping("/{memberId}")
    public ResponseEntity<Member> memberDelete() {
        return ResponseEntity.ok(new Member());
    }
}