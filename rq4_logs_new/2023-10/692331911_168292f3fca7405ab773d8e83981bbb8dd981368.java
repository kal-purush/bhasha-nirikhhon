package com.lotdiz.adminservice.controller.restcontroller;

import com.lotdiz.adminservice.dto.response.GetMakerResponseDto;
import com.lotdiz.adminservice.service.MakerInfoService;
import com.lotdiz.adminservice.utils.SuccessResponse;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MakerRestController {

    private final MakerInfoService makerInfoService;

  @GetMapping("/makers")
  public ResponseEntity<SuccessResponse<Map<String, List<GetMakerResponseDto>>>> getMakers(
      @PageableDefault(
              page = 0,
              size = 20,
              sort = {"createdAt"},
              direction = Sort.Direction.DESC)
          Pageable pageable) {
      List<GetMakerResponseDto> getMakerResponseDtos = makerInfoService.getMakers(pageable);

      return ResponseEntity.ok()
              .body(SuccessResponse.<Map<String, List<GetMakerResponseDto>>>builder()
                      .code(String.valueOf(HttpStatus.OK.value()))
                      .message(HttpStatus.OK.name())
                      .detail("메이커 조회 성공")
                      .data(Map.of("makers", getMakerResponseDtos))
                      .build());
  }

  //  @GetMapping("/makers/search")
  //  public ResponseEntity<SuccessResponse<Map<String, List<GetMakerSearchResponseDto>>>>
  // getMakers(
  //      @RequestParam("query") String query,
  //      @RequestParam("page") int page,
  //      @RequestParam("size") int size,
  //      @RequestParam("sort") String sort) {
  //    return ResponseEntity.ok().body(null);
  //  }
}