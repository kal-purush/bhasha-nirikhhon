package com.lotdiz.adminservice.controller.restcontroller;

import com.lotdiz.adminservice.dto.response.*;
import com.lotdiz.adminservice.service.MemberInfoService;
import com.lotdiz.adminservice.utils.SuccessResponse;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MemberRestController {

  private final MemberInfoService memberInfoService;

  @GetMapping("/members")
  public ResponseEntity<SuccessResponse<Map<String, List<GetMemberResponseDto>>>> getMakers(
      @PageableDefault(
              page = 0,
              size = 20,
              sort = {"createdAt"},
              direction = Sort.Direction.DESC)
          Pageable pageable) {
    List<GetMemberResponseDto> getMemberResponseDtos = memberInfoService.getMembers(pageable);
    return ResponseEntity.ok()
        .body(
            SuccessResponse.<Map<String, List<GetMemberResponseDto>>>builder()
                .code(String.valueOf(HttpStatus.OK.value()))
                .message(HttpStatus.OK.name())
                .detail("회원 조회 성공")
                .data(Map.of("members", getMemberResponseDtos))
                .build());
  }

  @GetMapping("/members/search")
  public ResponseEntity<SuccessResponse<Map<String, List<GetMemberSearchResponseDto>>>> getMakers(
      @RequestParam("query") String query,
      @PageableDefault(
              page = 0,
              size = 20,
              sort = {"createdAt"},
              direction = Sort.Direction.DESC)
          Pageable pageable) {
    return ResponseEntity.ok().body(null);
  }
}