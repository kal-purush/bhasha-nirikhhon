package com.mohamco.board.controller;

import com.mohamco.common.response.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class BoardController {

  @GetMapping("/v1/{boardSeq}")
  public ResponseEntity<?> getBoard() {
    return ResponseEntity.ok(BaseResponse.builder().build());
  }
}