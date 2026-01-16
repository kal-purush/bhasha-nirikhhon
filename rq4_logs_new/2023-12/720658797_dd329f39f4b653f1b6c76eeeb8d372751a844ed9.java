package com.jeontongju.product.controller;

import com.jeontongju.product.dto.response.GetShortsByConsumerDto;
import com.jeontongju.product.service.ShortsService;
import io.github.bitbox.bitbox.dto.ResponseFormat;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/api")
@RestController
@RequiredArgsConstructor
public class ShortsRestController {

  private final ShortsService shortsService;

  @GetMapping("/shorts")
  public ResponseEntity<ResponseFormat<List<GetShortsByConsumerDto>>> getMainShorts(
      @PageableDefault(page = 0, sort = "shortsHits", direction = Sort.Direction.DESC, size = 6)
          Pageable pageable) {

    return ResponseEntity.ok()
        .body(
            ResponseFormat.<List<GetShortsByConsumerDto>>builder()
                .code(HttpStatus.OK.value())
                .message(HttpStatus.OK.name())
                .detail("쇼츠 목록 조회 성공")
                .data(shortsService.getMainShorts(pageable))
                .build());
  }

  @GetMapping("/sellers/{sellerId}/shorts")
  public ResponseEntity<ResponseFormat<List<GetShortsByConsumerDto>>> getOneSellerShorts(
          @PathVariable Long sellerId,
          @PageableDefault(page = 0, sort = "shortsHits", direction = Sort.Direction.DESC, size = 5)
          Pageable pageable) {

    return ResponseEntity.ok()
            .body(
                    ResponseFormat.<List<GetShortsByConsumerDto>>builder()
                            .code(HttpStatus.OK.value())
                            .message(HttpStatus.OK.name())
                            .detail("셀러 소개 - 쇼츠 목록 조회 성공")
                            .data(shortsService.getOneSellerShorts(sellerId, pageable))
                            .build());
  }
}